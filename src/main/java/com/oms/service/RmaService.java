package com.oms.service;

import com.oms.api.dto.Requests.RmaCreateRequest;
import com.oms.api.dto.Views.RmaView;
import com.oms.common.*;
import com.oms.config.OmsProperties;
import com.oms.domain.*;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.repository.*;
import com.oms.statemachine.RmaStateMachine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/** Reverse logistics: RMA lifecycle REQUESTED -> APPROVED -> PICKUP_SCHEDULED -> QC_INSPECTION -> REFUNDED | EXCHANGED. */
@Slf4j
@Service
@RequiredArgsConstructor
public class RmaService {
    private static final List<RmaStatus> ACTIVE = List.of(RmaStatus.REQUESTED, RmaStatus.APPROVED, RmaStatus.PICKUP_SCHEDULED, RmaStatus.QC_INSPECTION);

    private final RmaRepository rmas;
    private final OrderRepository orders;
    private final ShipmentRepository shipments;
    private final OutboxService outbox;
    private final AuditService audit;
    private final OmsProperties props;
    private final RmaStateMachine sm;
    private final OrderService orderService;

    @Transactional
    public RmaView create(UUID orderId, RmaCreateRequest req, Actor actor) {
        Order o = orders.findByIdForUpdate(orderId).orElseThrow(() -> new NotFoundException("Order not found: " + orderId));
        if (o.getStatus() != OrderStatus.DELIVERED) throw new InvalidStateException("Returns are only accepted for DELIVERED orders");
        if (actor.type() == ActorType.CUSTOMER) {
            if (!o.getCustomerId().equals(actor.id())) throw new ForbiddenException("Order does not belong to this customer");
            if (o.getFulfilledAt() != null && Instant.now().isAfter(o.getFulfilledAt().plus(Duration.ofDays(props.rma().returnWindowDays()))))
                throw new InvalidStateException("Return window of " + props.rma().returnWindowDays() + " days has expired");
        }
        List<Shipment> ships = shipments.findByOrderId(orderId);
        Rma r = new Rma();
        r.setTenantId(o.getTenantId()); r.setOrderId(orderId); r.setRmaType(req.type()); r.setStatus(RmaStatus.REQUESTED);
        r.setReason(req.reason()); r.setCreatedBy(actor.id());
        r.setRmaNumber("RMA-" + String.format("%08d", rmas.nextRmaSeq()));
        for (RmaCreateRequest.Line l : req.items()) {
            OrderItem it = o.getItems().stream().filter(i -> i.getId().equals(l.orderItemId())).findFirst()
                .orElseThrow(() -> new NotFoundException("Order item not found: " + l.orderItemId()));
            long pending = rmas.pendingQty(it.getId(), ACTIVE);
            long available = it.effectiveQty() - it.getReturnedQty() - pending;
            if (l.quantity() > available) throw new InvalidStateException("Only " + available + " unit(s) of " + it.getSku() + " can still be returned");
            RmaItem ri = new RmaItem();
            ri.setOrderItemId(it.getId()); ri.setSku(it.getSku()); ri.setQuantity(l.quantity()); ri.setReplacementSku(l.replacementSku());
            ri.setReturnNodeId(routeReturn(it, ships));
            r.getItems().add(ri);
        }
        rmas.saveAndFlush(r);
        audit.record(o.getTenantId(), "RMA", r.getId().toString(), "RMA_REQUESTED", null, "REQUESTED", actor, req.type() + ": " + req.reason());
        return RmaView.from(r);
    }

    /** Item-level routing: electronics -> centralized reverse hub, everything else -> originating store/node. */
    private String routeReturn(OrderItem it, List<Shipment> ships) {
        if ("ELECTRONICS".equalsIgnoreCase(it.getCategory())) return props.rma().reverseHub();
        return ships.stream().filter(s -> s.getItems().stream().anyMatch(si ->
                Objects.equals(si.getSku(), it.getSku()) && Objects.equals(si.getWarehouseId(), it.getWarehouseId())))
            .map(Shipment::getFulfillmentShopId).findFirst().orElse(props.rma().reverseHub());
    }

    @Transactional(readOnly = true)
    public RmaView get(UUID id) { return RmaView.from(rmas.findById(id).orElseThrow(() -> new NotFoundException("RMA not found: " + id))); }

    @Transactional(readOnly = true)
    public List<RmaView> forOrder(UUID orderId) { return rmas.findByOrderId(orderId).stream().map(RmaView::from).toList(); }

    @Transactional
    public RmaView approve(UUID id, Actor actor) {
        actor.requireCsr();
        Rma r = load(id);
        move(r, RmaStatus.APPROVED, actor, null);
        Order o = orders.findById(r.getOrderId()).orElseThrow();
        outbox.enqueue(KafkaTopics.OMS_LOGISTICS_RETURN_PICKUP, r.getId().toString(), "ReturnPickupCommand", "Rma", r.getId().toString(), r.getTenantId(),
            new Events.ReturnPickupCommand(r.getId(), r.getOrderId(), r.getTenantId(), o.getShippingAddress(), lines(r)));
        return RmaView.from(r);
    }

    @Transactional
    public RmaView reject(UUID id, String reason, Actor actor) {
        actor.requireCsr();
        Rma r = load(id);
        move(r, RmaStatus.REJECTED, actor, reason);
        return RmaView.from(r);
    }

    /** CSR: re-emit the refund when the PSP reported a failure. */
    @Transactional
    public RmaView retryRefund(UUID id, Actor actor) {
        actor.requireCsr();
        Rma r = load(id);
        if (r.getStatus() != RmaStatus.QC_INSPECTION || r.getRefundTotal() == null) throw new InvalidStateException("No pending refund for this RMA");
        Order o = orders.findById(r.getOrderId()).orElseThrow();
        publishRefund(o, r, splitWallet(o, r.getRefundTotal()));
        audit.record(r.getTenantId(), "RMA", id.toString(), "REFUND_RETRIED", null, null, actor, null);
        return RmaView.from(r);
    }

    // ------------------------------------------------------------------ inbound (kafka)
    @Transactional
    public void onReturnPickupResult(Events.ReturnPickupResult res) {
        Rma r = load(res.rmaId());
        if ("SCHEDULED".equals(res.status())) {
            if (r.getStatus() == RmaStatus.APPROVED) move(r, RmaStatus.PICKUP_SCHEDULED, Actor.SYSTEM, "pickupAt=" + res.pickupAt());
        } else if ("DELIVERED_TO_HUB".equals(res.status())) {
            if (r.getStatus() == RmaStatus.APPROVED) move(r, RmaStatus.PICKUP_SCHEDULED, Actor.SYSTEM, "implicit");
            if (r.getStatus() != RmaStatus.PICKUP_SCHEDULED) return;
            move(r, RmaStatus.QC_INSPECTION, Actor.SYSTEM, "goods received at return node");
            outbox.enqueue(KafkaTopics.OMS_WMS_RETURN_INSPECTION, r.getId().toString(), "ReturnInspectionCommand", "Rma", r.getId().toString(), r.getTenantId(),
                new Events.ReturnInspectionCommand(r.getId(), r.getOrderId(), r.getTenantId(), lines(r)));
        }
    }

    @Transactional
    public void onReturnInspectionResult(Events.ReturnInspectionResult res) {
        Rma r = load(res.rmaId());
        if (r.getStatus() != RmaStatus.QC_INSPECTION) { log.warn("Stale inspection result for {}", r.getRmaNumber()); return; }
        Order o = orders.findByIdForUpdate(r.getOrderId()).orElseThrow();
        Map<UUID, Integer> accepted = new HashMap<>();
        for (Events.InspectionLine l : res.lines()) {
            r.getItems().stream().filter(i -> i.getOrderItemId().equals(l.orderItemId())).findFirst().ifPresent(i -> {
                i.setAcceptedQty(Math.min(l.acceptedQty(), i.getQuantity())); i.setRejectedQty(l.rejectedQty());
                accepted.merge(i.getOrderItemId(), i.getAcceptedQty(), Integer::sum);
            });
        }
        if (accepted.values().stream().mapToInt(Integer::intValue).sum() == 0) { move(r, RmaStatus.REJECTED, Actor.SYSTEM, "QC failed for all items"); return; }

        if (r.getRmaType() == RmaType.EXCHANGE) {
            List<OrderService.ExchangeLine> lines = new ArrayList<>();
            for (RmaItem ri : r.getItems()) {
                if (ri.getAcceptedQty() == 0) continue;
                OrderItem it = o.getItems().stream().filter(i -> i.getId().equals(ri.getOrderItemId())).findFirst().orElseThrow();
                it.setReturnedQty(it.getReturnedQty() + ri.getAcceptedQty());
                lines.add(new OrderService.ExchangeLine(it, ri.getAcceptedQty(), ri.getReplacementSku()));
            }
            Order child = orderService.createExchangeOrder(o, r, lines);
            r.setChildOrderId(child.getId());
            r.setRefundTotal(BigDecimal.ZERO.setScale(2));
            move(r, RmaStatus.EXCHANGED, Actor.SYSTEM, "child order " + child.getOrderNumber());
            return;
        }

        // ---- financial reconciliation: apportion item value, discounts/coupons, tax, delivery fee and wallet share
        BigDecimal items = BigDecimal.ZERO, disc = BigDecimal.ZERO, tax = BigDecimal.ZERO;
        List<Events.CreditMemoLine> memo = new ArrayList<>();
        boolean fullReturn = true;
        for (OrderItem it : o.getItems()) {
            int q = accepted.getOrDefault(it.getId(), 0);
            if (it.getReturnedQty() + q < it.effectiveQty()) fullReturn = false;
            if (q == 0) continue;
            BigDecimal sub = it.subtotalFor(q), d = it.discountFor(q), t = it.taxFor(q);
            items = items.add(sub); disc = disc.add(d); tax = tax.add(t);
            memo.add(new Events.CreditMemoLine(it.getId(), it.getSku(), q, sub, d, t));
            r.getItems().stream().filter(i -> i.getOrderItemId().equals(it.getId())).findFirst().ifPresent(i -> i.setRefundAmount(sub.subtract(d).add(t)));
            it.setReturnedQty(it.getReturnedQty() + q);
        }
        BigDecimal fee = fullReturn ? o.getDeliveryFee() : BigDecimal.ZERO.setScale(2);
        BigDecimal total = items.subtract(disc).add(tax).add(fee);
        BigDecimal[] split = splitWallet(o, total);                      // [wallet, original payment]
        r.setRefundTotal(total);
        outbox.enqueue(KafkaTopics.OMS_FINANCE_CREDIT_MEMO, r.getId().toString(), "CreditMemoEvent", "Rma", r.getId().toString(), r.getTenantId(),
            new Events.CreditMemoEvent(r.getId(), o.getId(), r.getId(), r.getTenantId(), o.getCurrency(), memo, items.subtract(disc), tax, fee,
                split[0], split[1], total));
        publishRefund(o, r, split);
        audit.record(r.getTenantId(), "RMA", r.getId().toString(), "REFUND_REQUESTED", "QC_INSPECTION", "QC_INSPECTION", Actor.SYSTEM, "total=" + total);
    }

    @Transactional
    public void onRefundResult(Events.PaymentRefundResult res) {
        Rma r = load(res.referenceId());
        if (res.success()) {
            if (r.getStatus() == RmaStatus.QC_INSPECTION) move(r, RmaStatus.REFUNDED, Actor.SYSTEM, "refundId=" + res.refundId());
        } else {
            audit.record(r.getTenantId(), "RMA", r.getId().toString(), "REFUND_FAILED", null, null, Actor.SYSTEM, res.reason());
        }
    }

    // ------------------------------------------------------------------ helpers
    private BigDecimal[] splitWallet(Order o, BigDecimal total) {
        BigDecimal wallet = o.getTotalAmount().signum() == 0 ? BigDecimal.ZERO
            : total.multiply(o.getWalletAmount()).divide(o.getTotalAmount(), 2, RoundingMode.HALF_UP).min(total);
        return new BigDecimal[]{wallet, total.subtract(wallet)};
    }

    private void publishRefund(Order o, Rma r, BigDecimal[] split) {
        outbox.enqueue(KafkaTopics.OMS_PAYMENT_PAYMENT_REFUND, o.getId().toString(), "PaymentRefundCommand", "Order", o.getId().toString(), o.getTenantId(),
            new Events.PaymentRefundCommand(o.getId(), o.getTenantId(), "RMA", r.getId(), o.getCurrency(), split[1], split[0], "Return " + r.getRmaNumber()));
    }

    private List<Events.ReturnLine> lines(Rma r) {
        return r.getItems().stream().map(i -> new Events.ReturnLine(i.getOrderItemId(), i.getSku(), i.getQuantity(), i.getReturnNodeId())).toList();
    }

    private Rma load(UUID id) { return rmas.findByIdForUpdate(id).orElseThrow(() -> new NotFoundException("RMA not found: " + id)); }

    private void move(Rma r, RmaStatus to, Actor actor, String details) {
        RmaStatus from = r.getStatus();
        sm.assertAllowed(from, to);
        r.setStatus(to);
        audit.record(r.getTenantId(), "RMA", r.getId().toString(), "RMA_STATUS_CHANGED", from.name(), to.name(), actor, details);
    }
}
