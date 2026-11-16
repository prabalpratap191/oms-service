package com.oms.service;

import com.oms.api.dto.CreateOrderRequest;
import com.oms.api.dto.Views;
import com.oms.api.dto.Requests.*;
import com.oms.api.dto.Views.*;
import com.oms.common.*;
import com.oms.domain.*;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    private final OrderRepository orders;
    private final ShipmentRepository shipments;
    private final OrderSagaRepository sagas;
    private final AuditLogRepository auditLogs;
    private final SagaOrchestrator saga;
    private final IdempotencyService idempotency;
    private final OutboxService outbox;
    private final AuditService audit;
    private final OmsMetrics metrics;
    private final TransactionTemplate tx;

    public record CreateResult(OrderView order, boolean created) {
    }

    public record ExchangeLine(OrderItem original, int quantity, String replacementSku) {
    }

    // ------------------------------------------------------------------ ingestion
    /** Idempotent ingestion for every channel (REST + marketplace Kafka). */
    public CreateResult createOrder(CreateOrderRequest req, Actor actor) {
        Order draft = buildOrder(req);
        String key = IdempotencyService.key(req.tenantId(), req.customerId(), req.cartToken(), draft.getTotalAmount());
        draft.setIdempotencyKey(key);

        OrderView dup = tx.execute(st -> orders.findByIdempotencyKey(key).map(this::toView).orElse(null));
        if (dup != null)
            return new CreateResult(dup, false);

        String token = idempotency.tryLock(key);
        if (token == null)
            throw new ConflictException("An identical order submission is already in progress");
        try {
            return tx.execute(st -> {
                Optional<Order> again = orders.findByIdempotencyKey(key);
                if (again.isPresent())
                    return new CreateResult(toView(again.get()), false);
                persistNew(draft, actor, "channel=" + draft.getChannel());
                return new CreateResult(toView(draft), true);
            });
        } catch (DataIntegrityViolationException dup2) { // lost a race against another pod: DB unique index wins
            OrderView v = tx.execute(st -> orders.findByIdempotencyKey(key).map(this::toView).orElseThrow(() -> dup2));
            return new CreateResult(v, false);
        } finally {
            idempotency.unlock(key, token);
        }
    }

    private void persistNew(Order o, Actor actor, String details) {
        o.setOrderNumber("ORD-" + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE) + "-"
                + String.format("%08d", orders.nextOrderSeq()));
        o.setStatus(OrderStatus.ORDER_CREATED);
        orders.saveAndFlush(o); // flush => item ids are generated
        OrderSaga s = new OrderSaga();
        s.setOrderId(o.getId());
        s.setStep(SagaStep.INVENTORY_CHECK);
        sagas.save(s);
        audit.record(o.getTenantId(), "ORDER", o.getId().toString(), "ORDER_CREATED", null, o.getStatus().name(), actor,
                details);

        outbox.enqueue(
            KafkaTopics.OMS_NOTIFICATION_ORDER_LIFECYCLE, 
            o.getId().toString(), 
            "OrderCreated",
             "Order",
                o.getId().toString(),
                o.getTenantId(),
                new Events.OrderLifecycleEvent(o.getId(), o.getOrderNumber(), o.getTenantId(), o.getCustomerId(), null,
                        o.getStatus().name(), actor.type().name(), actor.id(), details));
        metrics.orderCreated(o);
        saga.start(o, s);
    }

    private Order buildOrder(CreateOrderRequest r) {
        Order o = new Order();
        o.setTenantId(r.tenantId());
        o.setParentOrgId(r.parentOrgId());
        o.setSubOrgId(r.subOrgId());
        o.setCustomerId(r.customerId());
        o.setCustomerName(r.customerName() == null || r.customerName().isBlank() ? r.customerId() : r.customerName());
        o.setChannel(r.channel());
        o.setOrderType(r.orderType() == null ? OrderType.STANDARD : r.orderType());
        o.setStatus(OrderStatus.ORDER_CREATED);
        o.setPriority(r.priority() == null ? Priority.NORMAL : r.priority());
        o.setCurrency(r.currency().toUpperCase());
        o.setCartToken(r.cartToken());
        o.setExternalOrderRef(r.externalOrderRef());
        o.setRegionCode(r.regionCode());
        o.setShippingAddress(r.shippingAddress());
        if (r.paymentDetails() != null) {
            o.setPaymentMethod(r.paymentDetails().paymentMethod());
            o.setPaymentProvider(r.paymentDetails().paymentProvider());
            o.setPaymentStatus(r.paymentDetails().paymentStatus());
            o.setCardLast4(r.paymentDetails().cardLast4());
            o.setUtrReference(r.paymentDetails().utrReference());
            o.setGatewayTransactionId(r.paymentDetails().gatewayTransactionId());
            o.setAmountAuthorized(r.paymentDetails().amountAuthorized());
            o.setAmountCaptured(r.paymentDetails().amountCaptured());
            o.setAmountRefunded(r.paymentDetails().amountRefunded());
            o.setAmountNet(r.paymentDetails().amountNet());
        }
        o.setDeliverySlotStart(r.deliverySlotStart());
        o.setDeliverySlotEnd(r.deliverySlotEnd());
        o.setPickingCutoffAt(r.pickingCutoffAt() != null ? r.pickingCutoffAt() : Instant.now().plusSeconds(30 * 60));
        BigDecimal subtotal = BigDecimal.ZERO, discount = BigDecimal.ZERO, tax = BigDecimal.ZERO;
        for (CreateOrderRequest.Line l : r.items()) {
            // ---- pricing + tax snapshot at the exact moment of purchase (never re-read
            // from the catalog)
            OrderItem it = new OrderItem();
            it.setSku(l.sku());
            it.setProductName(l.productName());
            it.setCategory(l.category());
            it.setHsnCode(l.hsnCode());
            it.setWarehouseId(l.warehouseId());
            it.setOrderedQty(l.quantity());
            it.setAllocatedQty(0);
            it.setCancelledQty(0);
            it.setReturnedQty(0);
            it.setPackedQty(0);
            it.setStatus(OrderLineStatus.CREATED);
            it.setUnitPrice(scale(l.unitPrice()));
            BigDecimal gross = it.getUnitPrice().multiply(BigDecimal.valueOf(l.quantity()));
            BigDecimal disc = l.discountAmount() == null ? BigDecimal.ZERO : scale(l.discountAmount());
            if (disc.compareTo(gross) > 0)
                throw new IllegalArgumentException("Discount exceeds line amount for sku " + l.sku());
            BigDecimal rate = l.taxRate() == null ? BigDecimal.ZERO : l.taxRate();
            BigDecimal t = gross.subtract(disc).multiply(rate).divide(HUNDRED, 2, RoundingMode.HALF_UP);
            TaxType tt = l.taxType() != null ? l.taxType() : (rate.signum() == 0 ? TaxType.NONE : TaxType.CGST_SGST);
            BigDecimal cgst = BigDecimal.ZERO, sgst = BigDecimal.ZERO, igst = BigDecimal.ZERO;
            if (tt == TaxType.CGST_SGST) {
                cgst = t.divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
                sgst = t.subtract(cgst);
            } else if (tt == TaxType.IGST)
                igst = t;
            it.setDiscountAmount(disc);
            it.setTaxType(tt);
            it.setTaxRate(rate);
            it.setCgstAmount(cgst);
            it.setSgstAmount(sgst);
            it.setIgstAmount(igst);
            it.setTaxAmount(t);
            it.setLineTotal(gross.subtract(disc).add(t));
            o.getItems().add(it);
            subtotal = subtotal.add(gross);
            discount = discount.add(disc);
            tax = tax.add(t);
        }
        o.setSubtotal(subtotal);
        o.setDiscountTotal(discount);
        o.setTaxTotal(tax);
        o.setDeliveryFee(r.deliveryFee() == null ? BigDecimal.ZERO : scale(r.deliveryFee()));
        o.setWalletAmount(r.walletAmount() == null ? BigDecimal.ZERO : scale(r.walletAmount()));
        o.setTotalAmount(subtotal.subtract(discount).add(tax).add(o.getDeliveryFee()));
        return o;
    }

    /**
     * Zero-value child order linked to the parent transaction; runs inside the
     * caller's transaction.
     */
    public Order createExchangeOrder(Order parent, Rma rma, List<ExchangeLine> lines) {
        Order o = new Order();
        o.setTenantId(parent.getTenantId());
        o.setParentOrgId(parent.getParentOrgId());
        o.setSubOrgId(parent.getSubOrgId());
        o.setCustomerId(parent.getCustomerId());
        o.setChannel(parent.getChannel());
        o.setOrderType(OrderType.EXCHANGE);
        o.setParentOrderId(parent.getId());
        o.setStatus(OrderStatus.PENDING);
        o.setPriority(Priority.HIGH);
        o.setCurrency(parent.getCurrency());
        o.setRegionCode(parent.getRegionCode());
        o.setShippingAddress(parent.getShippingAddress());
        o.setCartToken("EXCHANGE-" + rma.getId());
        o.setPickingCutoffAt(Instant.now().plusSeconds(30 * 60));
        o.setIdempotencyKey(IdempotencyService.sha256("EXCHANGE|" + rma.getId()));
        for (ExchangeLine l : lines) {
            OrderItem it = new OrderItem();
            it.setSku(l.replacementSku() != null ? l.replacementSku() : l.original().getSku());
            it.setProductName(l.original().getProductName());
            it.setCategory(l.original().getCategory());
            it.setHsnCode(l.original().getHsnCode());
            it.setOrderedQty(l.quantity());
            it.setUnitPrice(BigDecimal.ZERO.setScale(2));
            it.setDiscountAmount(BigDecimal.ZERO.setScale(2));
            it.setTaxType(TaxType.NONE);
            it.setTaxRate(BigDecimal.ZERO);
            it.setCgstAmount(BigDecimal.ZERO);
            it.setSgstAmount(BigDecimal.ZERO);
            it.setIgstAmount(BigDecimal.ZERO);
            it.setTaxAmount(BigDecimal.ZERO.setScale(2));
            it.setLineTotal(BigDecimal.ZERO.setScale(2));
            o.getItems().add(it);
        }
        BigDecimal z = BigDecimal.ZERO.setScale(2);
        o.setSubtotal(z);
        o.setDiscountTotal(z);
        o.setTaxTotal(z);
        o.setDeliveryFee(z);
        o.setWalletAmount(z);
        o.setTotalAmount(z);
        persistNew(o, Actor.SYSTEM, "exchange for order " + parent.getOrderNumber() + " rma " + rma.getRmaNumber());
        return o;
    }

    // ------------------------------------------------------------------ queries
    @Transactional(readOnly = true)
    public OrderView get(UUID id) {
        return toView(find(id));
    }

    @Transactional(readOnly = true)
    public PageResult<OrderView> list(String tenantId, String customerId, int page, int size) {
        Page<Order> p = orders.findByTenantIdAndCustomerIdOrderByCreatedAtDesc(tenantId, customerId,
                PageRequest.of(page, Math.min(size, 100)));
        return new PageResult<>(p.getContent().stream().map(this::toView).toList(), p.getTotalElements(), page, size);
    }

    @Transactional(readOnly = true)
    public TimelineView timeline(UUID id) {
        Order o = find(id);
        List<Shipment> ships = shipments.findByOrderId(id);
        Map<String, Shipment> byId = new HashMap<>();
        List<String> ids = new ArrayList<>(List.of(id.toString()));
        ships.forEach(s -> {
            byId.put(s.getId().toString(), s);
            ids.add(s.getId().toString());
        });
        List<TimelineEntry> events = auditLogs.findByTenantIdAndAggregateIdInOrderByCreatedAtAsc(o.getTenantId(), ids)
                .stream().map(a -> {
                    Shipment s = byId.get(a.getAggregateId());
                    return new TimelineEntry(a.getCreatedAt(), a.getAggregateType(), a.getAction(), a.getFromStatus(),
                            a.getToStatus(),
                            a.getActorType() + ":" + a.getActorId(), a.getDetails(), s == null ? null : s.getId(),
                            s == null ? null : s.getTrackingNumber(), s == null ? null : s.getCourier(),
                            s == null ? null : s.getSlaWindowEnd());
                }).toList();
        return new TimelineView(o.getId(), o.getOrderNumber(), o.getStatus(),
                ships.stream().map(com.oms.api.dto.Views::shipment).toList(), events);
    }

    // ------------------------------------------------------------------ amendments
    @Transactional
    public OrderView updateDelivery(UUID id, UpdateDeliveryRequest req, Actor actor) {
        Order o = lockOwned(id, actor);
        if (o.getStatus() == OrderStatus.DELIVERED || o.getStatus() == OrderStatus.CANCELLED)
            throw new InvalidStateException("Order is " + o.getStatus());
        List<Shipment> ships = shipments.findByOrderId(id);
        boolean picked = ships.stream().anyMatch(s -> s.getStatus() == ShipmentStatus.PACKED
                || s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        boolean pastCutoff = o.getPickingCutoffAt() != null && Instant.now().isAfter(o.getPickingCutoffAt());
        if (picked || (pastCutoff && actor.type() != ActorType.CSR))
            throw new InvalidStateException("Picking cut-off has passed; delivery details can no longer be changed");
        if (req.shippingAddress() != null)
            o.setShippingAddress(req.shippingAddress());
        if (req.deliverySlotStart() != null)
            o.setDeliverySlotStart(req.deliverySlotStart());
        if (req.deliverySlotEnd() != null)
            o.setDeliverySlotEnd(req.deliverySlotEnd());
        audit.record(o.getTenantId(), "ORDER", id.toString(), "DELIVERY_UPDATED", o.getStatus().name(),
                o.getStatus().name(), actor, req.reason());
        for (Shipment s : ships) {
            if (s.getStatus() == ShipmentStatus.DISPATCH_REQUESTED) {
                outbox.enqueue(KafkaTopics.OMS_LOGISTICS_SHIPMENT_UPDATE, id.toString(), "ShipmentUpdateCommand",
                        "Order", id.toString(), o.getTenantId(),
                        new Events.ShipmentUpdateCommand(id, s.getId(), o.getShippingAddress(),
                                o.getDeliverySlotStart(), o.getDeliverySlotEnd()));
            }
        }
        return toView(o);
    }

    /**
     * Line-item removal / quantity reduction before warehouse release. Allowed only
     * while no reservation or payment is in flight
     * (PENDING awaiting sourcing, or RESERVED while held); once shipments are
     * released to nodes (DISPATCH_REQUESTED) use cancel / RMA.
     */
    @Transactional
    public OrderView amendItems(UUID id, AmendItemsRequest req, Actor actor) {
        Order o = lockOwned(id, actor);
        OrderSaga s = saga.sagaOf(id);
        boolean ok = (o.getStatus() == OrderStatus.PENDING && s.getStep() == SagaStep.INVENTORY_CHECK)
                || (o.getStatus() == OrderStatus.RESERVED && s.getStep() == SagaStep.SHIPMENT_DISPATCH);
        if (!ok)
            throw new InvalidStateException("Items can only be amended before warehouse release; current state "
                    + o.getStatus() + "/" + s.getStep());
        if (actor.type() == ActorType.CUSTOMER && o.getPickingCutoffAt() != null
                && Instant.now().isAfter(o.getPickingCutoffAt()))
            throw new InvalidStateException("Picking cut-off has passed; contact support");

        BigDecimal oldTotal = o.getTotalAmount();
        BigDecimal refundGross = BigDecimal.ZERO;
        Map<Shipment, List<Events.Line>> released = new LinkedHashMap<>();
        for (AmendItemsRequest.Change ch : req.changes()) {
            OrderItem it = o.getItems().stream().filter(i -> i.getId().equals(ch.orderItemId())).findFirst()
                    .orElseThrow(() -> new NotFoundException("Order item not found: " + ch.orderItemId()));
            int delta = it.effectiveQty() - ch.newQuantity();
            if (delta < 0)
                throw new IllegalArgumentException("Only quantity reductions are allowed (sku " + it.getSku() + ")");
            if (delta == 0)
                continue;
            it.setCancelledQty(it.getCancelledQty() + delta);
            int trim = Math.min(delta, it.getAllocatedQty());
            if (trim > 0) {
                trimAllocation(id, it, trim, released);
                it.setAllocatedQty(it.getAllocatedQty() - trim);
            }
            o.setSubtotal(o.getSubtotal().subtract(it.subtotalFor(delta)));
            o.setDiscountTotal(o.getDiscountTotal().subtract(it.discountFor(delta)));
            o.setTaxTotal(o.getTaxTotal().subtract(it.taxFor(delta)));
            refundGross = refundGross.add(it.netFor(delta));
        }
        o.recomputeTotals();
        audit.record(o.getTenantId(), "ORDER", id.toString(), "ITEMS_AMENDED", null, null, actor, req.reason());

        if (s.isInventoryReserved() && !released.isEmpty()) {
            List<Events.ShipmentAllocation> alloc = released.entrySet().stream()
                    .map(e -> new Events.ShipmentAllocation(e.getKey().getId(), e.getKey().getFulfillmentShopId(),
                            e.getValue()))
                    .toList();
            outbox.enqueue(KafkaTopics.OMS_WMS_INVENTORY_RELEASE, id.toString(), "InventoryReleaseCommand", "Order",
                    id.toString(), o.getTenantId(),
                    new Events.InventoryReleaseCommand(id, o.getTenantId(), "Line items reduced", alloc));
        }
        if (s.isPaymentCaptured() && refundGross.signum() > 0) { // partial refund, wallet portion pro-rata
            BigDecimal walletPart = oldTotal.signum() == 0 ? BigDecimal.ZERO
                    : refundGross.multiply(o.getWalletAmount()).divide(oldTotal, 2, RoundingMode.HALF_UP)
                            .min(o.getWalletAmount()).min(refundGross);
            o.setWalletAmount(o.getWalletAmount().subtract(walletPart));
            saga.refund(o, "AMENDMENT", UUID.randomUUID(), refundGross.subtract(walletPart), walletPart,
                    "Line items reduced");
        }
        if (o.getItems().stream().allMatch(i -> i.effectiveQty() == 0))
            saga.compensate(o, s, "ALL_ITEMS_REMOVED", actor);
        else if (o.getStatus() == OrderStatus.PENDING
                && o.getItems().stream().noneMatch(i -> i.effectiveQty() - i.getAllocatedQty() > 0)) {
            saga.emitCurrent(o, s); // nothing left to source => sourcing completes immediately
        }
        return toView(o);
    }

    private void trimAllocation(UUID orderId, OrderItem it, int qty, Map<Shipment, List<Events.Line>> released) {
        int remaining = qty;
        for (Shipment sh : shipments.findByOrderId(orderId)) {
            if (remaining == 0)
                break;
            if (sh.getStatus() != ShipmentStatus.PLANNED)
                continue;
            Iterator<ShipmentItem> iter = sh.getItems().iterator();
            while (iter.hasNext() && remaining > 0) {
                ShipmentItem si = iter.next();
                if (!Objects.equals(si.getSku(), it.getSku()) || !Objects.equals(si.getWarehouseId(), it.getWarehouseId()))
                    continue;
                int take = Math.min(si.getQuantity(), remaining);
                si.setQuantity(si.getQuantity() - take);
                remaining -= take;
                released.computeIfAbsent(sh, k -> new ArrayList<>())
                        .add(new Events.Line(it.getId(), it.getSku(), it.getCategory(), take));
                if (si.getQuantity() == 0)
                    iter.remove();
            }
            if (sh.getItems().isEmpty())
                sh.setStatus(ShipmentStatus.CANCELLED);
        }
    }

    private UUID resolveOrderItemId(Order order, ShipmentItem shipmentItem) {
        return order.getItems().stream()
            .filter(item -> Objects.equals(item.getSku(), shipmentItem.getSku())
                && Objects.equals(item.getWarehouseId(), shipmentItem.getWarehouseId()))
            .map(OrderItem::getId)
            .findFirst()
            .orElse(null);
    }

    // ------------------------------------------------------------------
    // cancellation
    @Transactional
    public OrderView cancel(UUID id, CancelRequest req, Actor actor) {
        Order o = lockOwned(id, actor);
        if (o.getStatus() == OrderStatus.DELIVERED || o.getStatus() == OrderStatus.CANCELLED)
            throw new InvalidStateException("Order is already " + o.getStatus());
        List<Shipment> ships = shipments.findByOrderId(id);
        boolean dispatched = ships.stream()
                .anyMatch(s -> s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        if (dispatched)
            throw new InvalidStateException("Shipment already dispatched; raise a return (RMA) instead");
        if (actor.type() == ActorType.CUSTOMER) { // self-serve policy
            boolean early = o.getStatus() == OrderStatus.PENDING || o.getStatus() == OrderStatus.SOURCED
                    || o.getStatus() == OrderStatus.RESERVED;
            boolean beforeCutoff = o.getPickingCutoffAt() == null || Instant.now().isBefore(o.getPickingCutoffAt());
            if (!early || !beforeCutoff)
                throw new InvalidStateException(
                        "Self-serve cancellation is closed (picking started / cut-off passed); contact support");
        } // CSR / SYSTEM override: any time before dispatch
        saga.compensate(o, saga.sagaOf(id),
                "CANCELLED_BY_" + actor.type() + (req != null && req.reason() != null ? ": " + req.reason() : ""),
                actor);
        return toView(o);
    }

    // ------------------------------------------------------------------ helpers
    private Order find(UUID id) {
        return orders.findById(id).orElseThrow(() -> new NotFoundException("Order not found: " + id));
    }

    private Order lockOwned(UUID id, Actor actor) {
        Order o = saga.lock(id);
        if (actor.type() == ActorType.CUSTOMER && !o.getCustomerId().equals(actor.id()))
            throw new ForbiddenException("Order does not belong to this customer");
        return o;
    }

    OrderView toView(Order o) {
        return OrderView.from(o, shipments.findByOrderId(o.getId()));
    }

    private static BigDecimal scale(BigDecimal v) {
        return v.setScale(2, RoundingMode.HALF_UP);
    }

    public List<Views.OrderSourcingRow> orderSourcing(String tenantId, String customerId) {
        List<Order> rows = orders.findAll().stream()
            .filter(o -> tenantId == null || tenantId.isBlank() || Objects.equals(o.getTenantId(), tenantId))
            .filter(o -> customerId == null || customerId.isBlank() || Objects.equals(o.getCustomerId(), customerId))
            .sorted(Comparator.comparing(Order::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
            .toList();

        return rows.stream()
            .map(Views.OrderSourcingRow::from)
            .toList();
    }

    public Views.OrderSourcingPage orderSourcing(String tenantId, String organizationId, String deliveryStartDate,
                                                String deliveryEndDate, String orderStatus, String paymentMethod,
                                                String orderType, String node, int page, int size) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("X-Tenant-Id is required");
        }
        if (organizationId == null || organizationId.isBlank()) {
            throw new IllegalArgumentException("X-Organization-Id is required");
        }

        List<Order> all = orders.findAll().stream()
            .filter(o -> Objects.equals(o.getTenantId(), tenantId))
            .filter(o -> organizationId == null || organizationId.isBlank() || Objects.equals(o.getParentOrgId(), organizationId) || Objects.equals(o.getSubOrgId(), organizationId))
            .filter(o -> deliveryStartDate == null || deliveryStartDate.isBlank() || o.getDeliverySlotStart() == null || !o.getDeliverySlotStart().isBefore(parseDateTime(deliveryStartDate)))
            .filter(o -> deliveryEndDate == null || deliveryEndDate.isBlank() || o.getDeliverySlotEnd() == null || !o.getDeliverySlotEnd().isAfter(parseDateTime(deliveryEndDate)))
            .filter(o -> orderStatus == null || orderStatus.isBlank() || (o.getStatus() != null && o.getStatus().name().equalsIgnoreCase(orderStatus)))
            .filter(o -> paymentMethod == null || paymentMethod.isBlank() || (o.getPaymentMethod() != null && o.getPaymentMethod().equalsIgnoreCase(paymentMethod)))
            .filter(o -> orderType == null || orderType.isBlank() || (o.getOrderType() != null && o.getOrderType().name().equalsIgnoreCase(orderType)))
            .filter(o -> node == null || node.isBlank() || (o.getShippingAddress() != null && o.getShippingAddress().city() != null && o.getShippingAddress().city().equalsIgnoreCase(node)))
            .sorted(Comparator.comparing(Order::getCreatedAt, Comparator.nullsLast(Comparator.reverseOrder())))
            .toList();

        int safePage = Math.max(page, 0);
        int safeSize = size <= 0 ? 20 : size;
        int from = Math.min(safePage * safeSize, all.size());
        int to = Math.min(from + safeSize, all.size());

        List<Views.OrderSourcingRow> rows = all.subList(from, to).stream()
            .map(Views.OrderSourcingRow::from)
            .toList();

        return new Views.OrderSourcingPage(rows, all.size(), safePage, safeSize);
    }

    private Instant parseDateTime(String value) {
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            try {
                return java.time.LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant();
            } catch (Exception innerIgnored) {
                return Instant.EPOCH;
            }
        }
    }

    @Transactional(readOnly = true)
    public Views.OrderDetailView getDetailed(UUID id) {
        Order o = find(id);
        List<Shipment> ships = shipments.findByOrderId(id);
        List<ShipmentItemView> shipmentLines = ships.stream()
            .flatMap(s -> s.getItems().stream().map(si -> new ShipmentItemView(si.getSku(), si.getQuantity(), si.getPickedQuantity(), si.getWarehouseId())))
            .toList();
        List<AuditLog> logs = auditLogs.findByTenantIdAndAggregateIdInOrderByCreatedAtAsc(o.getTenantId(), List.of(id.toString()));
        List<AuditLogView> auditLogViews = logs.stream().map(AuditLogView::from).toList();
        List<TimelineEntry> statusTimeline = logs.stream().map(a -> new TimelineEntry(a.getCreatedAt(), a.getAggregateType(), a.getAction(), a.getFromStatus(), a.getToStatus(),
            a.getActorType() == null ? null : a.getActorType().name() + ":" + a.getActorId(), a.getDetails(), null, null, null, null)).toList();

        List<ItemView> itemLines = o.getItems().stream().map(i -> new ItemView(i.getId(), i.getSku(), i.getProductName(), i.getCategory(),
            i.getHsnCode(), i.getOrderedQty(), i.getAllocatedQty(), i.getCancelledQty(), i.getReturnedQty(), i.getUnitPrice(),
            i.getDiscountAmount(), i.getTaxType(), i.getTaxRate(), i.getCgstAmount(), i.getSgstAmount(), i.getIgstAmount(),
            i.getTaxAmount(), i.getLineTotal())).toList();

        return new Views.OrderDetailView(o.getId(), o.getOrderNumber(), o.getTenantId(), o.getParentOrgId(), o.getSubOrgId(), o.getCustomerId(),
            o.getCustomerName(), o.getChannel(), o.getOrderType(), o.getParentOrderId(), o.getStatus(), o.getPriority(), o.isOnHold(),
            o.getCurrency(), o.getSubtotal(), o.getDiscountTotal(), o.getTaxTotal(), o.getDeliveryFee(), o.getWalletAmount(), o.getTotalAmount(),
            o.getPaymentMethod(), o.getPaymentProvider(), o.getPaymentStatus(), o.getCardLast4(), o.getUtrReference(), o.getGatewayTransactionId(),
            o.getAmountAuthorized(), o.getAmountCaptured(), o.getAmountRefunded(), o.getAmountNet(), o.getShippingAddress(),
            o.getDeliverySlotStart(), o.getDeliverySlotEnd(), o.getPickingCutoffAt(), o.getCreatedAt(),
            itemLines, ships.stream().map(Views::shipment).toList(), shipmentLines, auditLogViews, statusTimeline);
    }
}
