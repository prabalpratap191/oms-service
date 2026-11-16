package com.oms.service;

import com.oms.common.Actor;
import com.oms.common.NotFoundException;
import com.oms.config.OmsProperties;
import com.oms.domain.*;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.repository.*;
import com.oms.statemachine.OrderStateMachine;
import com.oms.statemachine.ShipmentStateMachine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Orchestrated saga for the post-click lifecycle. Every outgoing command goes
 * through the transactional outbox.
 *
 * PENDING --inventoryCheck--> SOURCED --inventoryReserve--> (paymentCapture)
 * RESERVED --shipmentCreate--> SPLIT_PROCESSING --> FULFILLED
 * Any failure => compensate(): InventoryReleaseCommand / PaymentRefundCommand /
 * ShipmentCancelCommand, order -> CANCELLED.
 * A CSR hold pauses *outgoing* commands; releasing the hold re-emits the
 * command of the current step.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {
    private final OrderRepository orders;
    private final ShipmentRepository shipments;
    private final OrderSagaRepository sagas;
    private final OutboxService outbox;
    private final AuditService audit;
    private final OmsProperties props;
    private final OmsMetrics metrics;
    private final OrderStateMachine orderSm;
    private final ShipmentStateMachine shipmentSm;

    // ------------------------------------------------------------------ helpers
    public Order lock(UUID id) {
        return orders.findByIdForUpdate(id).orElseThrow(() -> new NotFoundException("Order not found: " + id));
    }

    public OrderSaga sagaOf(UUID id) {
        return sagas.findById(id).orElseThrow(() -> new NotFoundException("Saga not found: " + id));
    }

    private void send(Order o, String topic, String type, Object payload) {
        outbox.enqueue(topic, o.getId().toString(), type, "Order", o.getId().toString(), o.getTenantId(), payload);
    }

    public void transition(Order o, OrderStatus to, Actor actor, String reason) {
        OrderStatus from = o.getStatus();
        if (from == to) {
            log.info("Order {} already in status {}. Skipping transition to {}", o.getId(), from, to);
            return;
        }
        orderSm.assertAllowed(from, to);
        o.setStatus(to);
        audit.record(o.getTenantId(), "ORDER", o.getId().toString(), "STATUS_CHANGED", from.name(), to.name(), actor,
                reason);
        send(o, KafkaTopics.OMS_NOTIFICATION_ORDER_LIFECYCLE, "OrderStatusChanged", new Events.OrderLifecycleEvent(
                o.getId(), o.getOrderNumber(), o.getTenantId(), o.getCustomerId(), from.name(), to.name(),
                actor.type().name(), actor.id(), reason));
        metrics.statusChanged(to);
    }

    private OrderStatus deriveOrderStatus(List<Shipment> active) {
        if (active == null || active.isEmpty()) return null;

        boolean allDelivered = active.stream().allMatch(s -> s.getStatus() == ShipmentStatus.DELIVERED);
        if (allDelivered) return OrderStatus.DELIVERED;

        boolean anyDelivered = active.stream().anyMatch(s -> s.getStatus() == ShipmentStatus.DELIVERED);
        if (anyDelivered) return OrderStatus.PARTIALLY_DELIVERED;

        boolean allDispatched = active.stream().allMatch(s -> s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        if (allDispatched) return OrderStatus.DISPATCHED;

        boolean anyDispatched = active.stream().anyMatch(s -> s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        if (anyDispatched) return OrderStatus.PARTIALLY_DISPATCHED;

        boolean allShipped = active.stream().allMatch(s -> s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        if (allShipped) return OrderStatus.SHIPPED;

        boolean anyShipped = active.stream().anyMatch(s -> s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        if (anyShipped) return OrderStatus.PARTIALLY_SHIPPED;

        return OrderStatus.READY_TO_SHIP;
    }

    private void shipmentTransition(Order o, Shipment s, ShipmentStatus to, Actor actor, String details) {
        ShipmentStatus from = s.getStatus();
        if (from == to) {
            log.info("Shipment {} already in status {}. Skipping transition to {}", s.getId(), from, to);
            return;
        }
        shipmentSm.assertAllowed(from, to);
        s.setStatus(to);
        audit.record(o.getTenantId(), "SHIPMENT", s.getId().toString(), "SHIPMENT_STATUS_CHANGED", from.name(),
                to.name(), actor, details);
    }

    private List<Events.ShipmentAllocation> allocations(Order o, List<Shipment> ships) {
        return ships.stream().map(sh -> new Events.ShipmentAllocation(sh.getId(), sh.getFulfillmentShopId(),
                sh.getItems().stream().map(i -> new Events.Line(resolveOrderItemId(o, i), i.getSku(), null, i.getQuantity()))
                        .toList()))
                .toList();
    }

    private UUID resolveOrderItemId(Order order, ShipmentItem item) {
        return order.getItems().stream()
                .filter(candidate -> Objects.equals(candidate.getSku(), item.getSku())
                        && Objects.equals(candidate.getWarehouseId(), item.getWarehouseId()))
                .map(OrderItem::getId)
                .findFirst()
                .orElse(null);
    }

    // ------------------------------------------------------------------ step
    // emission
    @Async 
    public void start(Order o, OrderSaga s) {
        s.setStep(SagaStep.INVENTORY_CHECK);
        emitCurrent(o, s);
    }

    @Transactional
    public void resume(UUID orderId) {
        Order o = lock(orderId);
        emitCurrent(o, sagaOf(orderId));
    }

    public void emitCurrent(Order o, OrderSaga s) {
        if (o.isOnHold()) {
            log.info("Order {} on hold; step {} paused", o.getOrderNumber(), s.getStep());
            return;
        }
        log.info("emitCurrent: order {} step {}", o.getOrderNumber(), s.getStep());
        switch (s.getStep()) {
            case INVENTORY_CHECK -> emitInventoryCheck(o, s);
            case INVENTORY_RESERVE -> emitInventoryReserve(o);
            case PAYMENT_CAPTURE -> emitPaymentCapture(o, s);
            case SHIPMENT_PICK_PACK -> emitShipmentPickPack(o, s);
            case SHIPMENT_DISPATCH -> emitShipmentDispatch(o, s);
            default -> {
                log.warn("emitCurrent: order {} step {}", o.getOrderNumber(), s.getStep());
            }
        }
    }

    private void emitShipmentPickPack(Order o, OrderSaga s) {
        log.info("Emitting shipment pick/pack event for order {}", o.getOrderNumber());
        log.info("Order {} has {} shipments", o.getOrderNumber(), shipments.findByOrderId(o.getId()).size());
        log.info("Order {} has {} items", o.getOrderNumber(), o.getItems().size());
        // s.setStep(SagaStep.WAIT_FOR_PICK_PACK);
        markShipmentsReadyToPick(o);
        log.info("Order {} has {} Status", o.getOrderNumber(), o.getStatus());
        shipments.findByOrderId(o.getId()).stream()
                .filter(sh -> sh.getStatus() == ShipmentStatus.PLANNED)
                .forEach(sh -> {
                    shipmentTransition(o, sh, ShipmentStatus.READY_TO_PICK, Actor.SYSTEM, "Ready for pick/pack");
                    send(o, KafkaTopics.OMS_LOGISTICS_SHIPMENT_PICK_PACK, "ShipmentPickPackCommand",
                            new Events.ShipmentPickPackCommand(
                                    o.getId(), sh.getId(), o.getTenantId(), sh.getFulfillmentShopId(),
                                    sh.getItems().stream()
                                            .map(i -> new Events.Line(resolveOrderItemId(o, i), i.getSku(), null, i.getQuantity()))
                                            .toList()));
                });

    }
    private void markShipmentsReadyToPick(Order o) {
        log.info("Marking shipments ready to pick for order {}", o.getOrderNumber());

       List<Shipment> shipmentList = shipments.findByOrderId(o.getId());
        shipmentList.stream()
                .filter(sh -> sh.getStatus() == ShipmentStatus.PLANNED)
                .forEach(sh -> {
                    sh.setStatus(ShipmentStatus.READY_TO_PICK);
                    sh.getItems().forEach(i -> i.setStatus(ShipmentLineStatus.READY_TO_PICK));
                    shipmentTransition(o, sh, ShipmentStatus.READY_TO_PICK, Actor.SYSTEM, "Ready for pick/pack");
                });
        shipments.saveAll(shipmentList);
    }

    private void emitInventoryCheck(Order o, OrderSaga s) {
        List<Events.Line> lines = o.getItems().stream()
                .filter(i -> i.effectiveQty() - i.getAllocatedQty() > 0)
                .map(i -> new Events.Line(i.getId(), i.getSku(), i.getCategory(),
                        i.effectiveQty() - i.getAllocatedQty()))
                .toList();
        if (lines.isEmpty()) {
            completeSourcing(o, s);
            return;
        }
        send(o, KafkaTopics.OMS_WMS_INVENTORY_CHECK, "InventoryCheckCommand",
                new Events.InventoryCheckCommand(o.getId(), o.getTenantId(), o.getRegionCode(), s.excludedNodeList(),
                        lines));
    }

    private void completeSourcing(Order o, OrderSaga s) {
        if (o.getStatus() == OrderStatus.PENDING || o.getStatus() == OrderStatus.ORDER_CREATED)
            transition(o, OrderStatus.PENDING, Actor.SYSTEM, "All lines sourced");
        s.setStep(SagaStep.INVENTORY_RESERVE);
        emitCurrent(o, s);
    }

    private void emitInventoryReserve(Order o) {
        List<Shipment> ships = shipments.findByOrderId(o.getId()).stream()
                .filter(sh -> sh.getStatus() == ShipmentStatus.PLANNED).toList();
        send(o, KafkaTopics.OMS_WMS_INVENTORY_RESERVE, "InventoryReserveCommand",
                new Events.InventoryReserveCommand(o.getId(), o.getTenantId(), allocations(o, ships)));
    }

    private void emitPaymentCapture(Order o, OrderSaga s) {
        if (o.payable().signum() == 0 && o.getWalletAmount().signum() == 0) { // zero-value (exchange) orders skip
                                                                              // payment
            afterPayment(o, s);
            return;
        }
        send(o, KafkaTopics.OMS_PAYMENT_PAYMENT_CAPTURE, "PaymentCaptureCommand", new Events.PaymentCaptureCommand(
                o.getId(), o.getTenantId(), o.getCustomerId(), o.getCurrency(), o.payable(), o.getWalletAmount()));
    }

    private void afterPayment(Order o, OrderSaga s) {
        s.setPaymentCaptured(o.payable().signum() > 0 || o.getWalletAmount().signum() > 0);
        transition(o, OrderStatus.RESERVED, Actor.SYSTEM, "Inventory reserved and payment captured");
        s.setStep(SagaStep.SHIPMENT_PICK_PACK);
        emitCurrent(o, s);
    }

    private void emitShipmentDispatch(Order o, OrderSaga s) {
        if (o.getStatus() == OrderStatus.RESERVED)
            transition(o, OrderStatus.SPLIT_PROCESSING, Actor.SYSTEM, "Released to fulfillment nodes");
        List<Shipment> planned = shipments.findByOrderId(o.getId()).stream()
                .filter(sh -> sh.getStatus() == ShipmentStatus.PLANNED).toList();
        for (Shipment sh : planned) {
            shipmentTransition(o, sh, ShipmentStatus.DISPATCH_REQUESTED, Actor.SYSTEM,
                    "node=" + sh.getFulfillmentShopId());
            send(o, KafkaTopics.OMS_LOGISTICS_SHIPMENT_CREATE, "ShipmentCreateCommand",
                    new Events.ShipmentCreateCommand(
                            o.getId(), sh.getId(), o.getTenantId(), sh.getFulfillmentShopId(),
                            sh.getDeliveryClass().name(), sh.getSlaWindowEnd(),
                            o.getPriority().name(), o.getShippingAddress(),
                            sh.getItems().stream()
                                    .map(i -> new Events.Line(resolveOrderItemId(o, i), i.getSku(), null, i.getQuantity()))
                                    .toList()));
        }
        long active = shipments.findByOrderId(o.getId()).stream()
                .filter(sh -> sh.getStatus() != ShipmentStatus.CANCELLED).count();
        if (!planned.isEmpty())
            metrics.splitRatio(o, (int) active);
    }

    // ------------------------------------------------------------------ inbound
    // results
    @Transactional
    public void onInventoryCheckResult(Events.InventoryCheckResult r) {
        log.info("onInventoryCheckResult: order {} allocated={} unfulfilled={}", r.orderId(),
                 r.allocated(), r.unfulfilled());
        Order o = lock(r.orderId());
        OrderSaga s = sagaOf(o.getId());
        if ((o.getStatus() != OrderStatus.PENDING && o.getStatus() != OrderStatus.ORDER_CREATED) || s.getStep() != SagaStep.INVENTORY_CHECK) {
            log.warn("Stale inventoryCheckResult for {}", o.getOrderNumber());
            return;
        }

        splitOrderByWarehouse(o);
        // Map<UUID, OrderItem> byId = new HashMap<>();
        // o.getItems().forEach(i -> byId.put(i.getId(), i));
        // if (r.allocated() != null) {
        //     log.info("onInventoryCheckResult: order {} has allocated items", r.orderId());
        //     for (Events.Allocation a : r.allocated()) {
        //         OrderItem item = byId.get(a.orderItemId());
        //         log.info("onInventoryCheckResult: order {} item {} warehouse {} quantity {}", r.orderId(), a.orderItemId(), item.getWarehouseId(), a.quantity());
        //         if (item == null)
        //             continue;
        //         int q = Math.min(a.quantity(), item.effectiveQty() - item.getAllocatedQty());   // cap against concurrent amendments
        //         if (q <= 0) continue;
        //         shipmentFor(o, a.nodeId(), item.getCategory(), item).addItem(item.getSku(), q);
        //         item.setAllocatedQty(item.getAllocatedQty() + q);
        //     }
        // }
        // boolean shortfall = o.getItems().stream().anyMatch(i -> i.effectiveQty() - i.getAllocatedQty() > 0);
        // if (!shortfall) {
        //     completeSourcing(o, s);
        //     return;
        // }

        // // dynamic re-sourcing: exclude the nodes that stocked out / reported damage,
        // // then ask again
        // if (r.unfulfilled() != null)
        //     r.unfulfilled().forEach(u -> s.exclude(u.nodeId()));
        // s.setResourcingAttempts(s.getResourcingAttempts() + 1);
        // if (s.getResourcingAttempts() > props.sourcing().maxResourcingAttempts()) {
        //     compensate(o, s, "UNFULFILLABLE: sourcing exhausted after " + props.sourcing().maxResourcingAttempts()
        //             + " re-sourcing attempts", Actor.SYSTEM);
        //     return;
        // }
        audit.record(o.getTenantId(), "ORDER", o.getId().toString(), "RE_SOURCING", null, null, Actor.SYSTEM,
                "attempt=" + s.getResourcingAttempts() + " excluded=" + s.getExcludedNodes());
        s.setStep(SagaStep.INVENTORY_RESERVE);
        s.setInventoryReserved(true);
        completeSourcing(o, s);
        emitCurrent(o, s);
    }


    public void splitOrderByWarehouse(Order order) {
        if (order == null || order.getItems() == null || order.getItems().isEmpty()) {
            log.warn("splitOrderByWarehouse: order {} has no items to split", order != null ? order.getId() : "null");
            return;
        }

        // Group items by warehouseId while preserving insertion order
        Map<String, List<OrderItem>> itemsByWarehouse = order.getItems().stream()
            .collect(Collectors.groupingBy(
                OrderItem::getWarehouseId,
                LinkedHashMap::new,
                Collectors.toList()
            ));

        // Convert each warehouse group into a Shipment object
      itemsByWarehouse.entrySet().stream()
            .map(entry -> shipmentFor(order, entry.getKey(), entry.getValue().get(0).getCategory() , entry.getValue()))
            .toList();
            
    }

    

    private Shipment shipmentFor(Order o, String nodeId, String category, List<OrderItem> items ) {
        DeliveryClass dc = DeliveryClass.forCategory(category);
        log.info("shipmentFor: order {} node {} category {}", o.getOrderNumber(), nodeId, category);
           
            Shipment sh = new Shipment();
            sh.setOrderId(o.getId()); 
            sh.setTenantId(o.getTenantId()); 
            sh.setFulfillmentShopId(nodeId);
            sh.setWarehouseId(nodeId);
            sh.setDeliveryClass(dc); 
            sh.setStatus(ShipmentStatus.PLANNED);
            sh.setShippingAddress(o.getShippingAddress());
            sh.setPickupAddress(o.getOrderType() == OrderType.PICKUP_POINT || o.getOrderType() == OrderType.STORE_PICKUP ? o.getShippingAddress() : null);
            sh.setSubtotal(BigDecimal.ZERO.setScale(2));
            sh.setTaxAmount(BigDecimal.ZERO.setScale(2));
            sh.setDeliveryFee(o.getDeliveryFee());
            sh.setTotalAmount(o.getTotalAmount());
            sh.setSlaWindowEnd(Instant.now().plusSeconds(dc.slaMinutes() * 60L));
            attachShipmentLine(sh, items);
            shipments.save(sh);
           
            log.info("shipmentForCreated for: order {} node {} category {} created shipment {}", o.getOrderNumber(), nodeId, category, sh.getId());
           
        return sh;
    }

    private void attachShipmentLine(Shipment sh, List<OrderItem> items) {
        items.forEach(item -> {
            int quantity = item.effectiveQty() - item.getAllocatedQty();
            if (quantity <= 0) return;
            ShipmentItem si = new ShipmentItem();
            si.setSku(item.getSku());
            si.setProductName(item.getProductName());
            si.setCategory(item.getCategory());
            si.setHsnCode(item.getHsnCode());
            si.setWarehouseId(item.getWarehouseId() != null ? item.getWarehouseId() : "");
            si.setOrderedQty(item.getOrderedQty());
            si.setAllocatedQty(item.getAllocatedQty());
            si.setCancelledQty(item.getCancelledQty());
            si.setReturnedQty(item.getReturnedQty());
            si.setQuantity(quantity);
            si.setPickedQuantity(0);
            si.setPackedQty(0);
            si.setUnitPrice(item.getUnitPrice());
            si.setDiscountAmount(item.getDiscountAmount()== null ? BigDecimal.ZERO : item.getDiscountAmount());
            si.setTaxType(item.getTaxType());
            si.setTaxRate(item.getTaxRate());
            si.setCgstAmount(item.getCgstAmount());
            si.setSgstAmount(item.getSgstAmount());
            si.setIgstAmount(item.getIgstAmount());
            si.setTaxAmount(item.getTaxAmount());
            si.setLineTotal(item.getLineTotal());
            si.setStatus(ShipmentLineStatus.PLANNED);
            sh.getItems().add(si);
        });
        log.info("attachShipmentLine: order {} shipment {} items {}", sh.getOrderId(), sh.getId(), items.size());
        
    }

    @Transactional
    public void onInventoryReserveResult(Events.InventoryReserveResult r) {
        Order o = lock(r.orderId());
        OrderSaga s = sagaOf(o.getId());
        if (o.getStatus() == OrderStatus.CANCELLED) { // cancelled while the reserve was in flight
            if (r.success() && !s.isInventoryReserved())
                releaseAll(o, "Late reservation after cancellation");
            return;
        }
        if (o.getStatus() != OrderStatus.SOURCED || s.getStep() != SagaStep.INVENTORY_RESERVE) {
            log.warn("Stale inventoryReserveResult for {}", o.getOrderNumber());
            return;
        }
        if (!r.success()) {
            compensate(o, s, "INVENTORY_RESERVE_FAILED: " + r.reason(), Actor.SYSTEM);
            return;
        }
        s.setInventoryReserved(true);
        s.setStep(SagaStep.PAYMENT_CAPTURE);
        emitCurrent(o, s);
    }

    @Transactional
    public void onPaymentCaptureResult(Events.PaymentCaptureResult r) {
        Order o = lock(r.orderId());
        OrderSaga s = sagaOf(o.getId());
        if (o.getStatus() == OrderStatus.CANCELLED) {
            if (r.success() && !s.isPaymentCaptured())
                refund(o, "ORDER_CANCEL", o.getId(), o.payable(), o.getWalletAmount(),
                        "Late capture after cancellation");
            return;
        }
        if (o.getStatus() != OrderStatus.SOURCED || s.getStep() != SagaStep.PAYMENT_CAPTURE) {
            log.warn("Stale paymentCaptureResult for {}", o.getOrderNumber());
            return;
        }
        if (!r.success()) {
            compensate(o, s, "PAYMENT_FAILED: " + r.reason(), Actor.SYSTEM);
            return;
        }
        afterPayment(o, s);
    }

    /**
     * CSR-triggered reconciliation: if the PSP says CAPTURED but we never saw the
     * response, move on.
     */
    @Transactional
    public void onPaymentReconcileResult(Events.PaymentReconcileResult r) {
        Order o = lock(r.orderId());
        OrderSaga s = sagaOf(o.getId());
        audit.record(o.getTenantId(), "ORDER", o.getId().toString(), "PAYMENT_RECONCILED", null, null, Actor.SYSTEM,
                r.status() + " amount=" + r.capturedAmount());
        if ("CAPTURED".equals(r.status()) && o.getStatus() == OrderStatus.SOURCED
                && s.getStep() == SagaStep.PAYMENT_CAPTURE)
            afterPayment(o, s);
    }

    @Transactional
    public void onShipmentCreateResult(Events.ShipmentCreateResult r) {
        Order o = lock(r.orderId());
        Shipment sh = shipments.findById(r.shipmentId())
                .orElseThrow(() -> new NotFoundException("Shipment not found: " + r.shipmentId()));
        if (!r.success()) {
            audit.record(o.getTenantId(), "SHIPMENT", sh.getId().toString(), "SHIPMENT_CREATE_FAILED", null, null,
                    Actor.SYSTEM, r.reason());
            return; // surfaced to CSR; carrier failures are handled manually
        }
        sh.setCourier(r.courier());
        sh.setTrackingNumber(r.trackingNumber());
        audit.record(o.getTenantId(), "SHIPMENT", sh.getId().toString(), "TRACKING_ASSIGNED", null,
                sh.getStatus().name(), Actor.SYSTEM,
                "courier=" + r.courier() + " tracking=" + r.trackingNumber());
    }

    @Transactional
    public void onShipmentStatus(Events.ShipmentStatusUpdate u) {
        Shipment probe = shipments.findById(u.shipmentId())
                .orElseThrow(() -> new NotFoundException("Shipment not found: " + u.shipmentId()));
        Order o = lock(probe.getOrderId());
        Shipment sh = shipments.findById(u.shipmentId()).orElseThrow();

        ShipmentStatus to;
        try {
            to = ShipmentStatus.valueOf(u.status());
        } catch (IllegalArgumentException e) {
            log.warn("Unknown shipment status {}", u.status());
            return;
        }

        if (sh.getStatus() == to) {
            log.info("Ignoring shipment status {} -> {} for {}", sh.getStatus(), to, sh.getId());
            return;
        }
        if (!shipmentSm.canTransition(sh.getStatus(), to)) {
            log.info("Ignoring shipment status {} -> {} for {}", sh.getStatus(), to, sh.getId());
            return;
        }

        Instant at = u.occurredAt() == null ? Instant.now() : u.occurredAt();
        shipmentTransition(o, sh, to, Actor.SYSTEM, "carrier/node update");

        if (to == ShipmentStatus.PACKED) {
            sh.getItems().forEach(i -> i.setPickedQuantity(i.getQuantity()));
        }
        if (to == ShipmentStatus.DISPATCHED) {
            sh.setDispatchedAt(at);
        }
        if (to == ShipmentStatus.DELIVERED) {
            sh.setDeliveredAt(at);
        }

        List<Shipment> active = shipments.findByOrderId(o.getId()).stream()
                .filter(x -> x.getStatus() != ShipmentStatus.CANCELLED)
                .toList();

        if (active.isEmpty()) {
            return;
        }

        OrderStatus target = deriveOrderStatus(active);
        if (target == null) {
            return;
        }

        if (target == OrderStatus.DELIVERED) {
            if (o.getStatus() != OrderStatus.DELIVERED) {
                o.setFulfilledAt(at);
                transition(o, OrderStatus.DELIVERED, Actor.SYSTEM, "All shipments delivered");
                sagaOf(o.getId()).setStep(SagaStep.COMPLETED);
                metrics.clickToDeliver(o, at);
            }
            if (o.getDispatchedAt() == null) {
                o.setDispatchedAt(at);
                metrics.orderToFulfill(o, at);
            }
            return;
        }

        if (target == OrderStatus.PARTIALLY_DELIVERED) {
            if (o.getStatus() != OrderStatus.PARTIALLY_DELIVERED && o.getStatus() != OrderStatus.DELIVERED) {
                transition(o, OrderStatus.PARTIALLY_DELIVERED, Actor.SYSTEM, "One or more shipments delivered");
            }
            if (o.getDispatchedAt() == null) {
                o.setDispatchedAt(at);
                metrics.orderToFulfill(o, at);
            }
            return;
        }

        if (target == OrderStatus.DISPATCHED) {
            if (o.getStatus() != OrderStatus.DISPATCHED && o.getStatus() != OrderStatus.DELIVERED) {
                transition(o, OrderStatus.DISPATCHED, Actor.SYSTEM, "All shipments dispatched");
            }
            if (o.getDispatchedAt() == null) {
                o.setDispatchedAt(at);
                metrics.orderToFulfill(o, at);
            }
            return;
        }

        if (target == OrderStatus.PARTIALLY_DISPATCHED) {
            if (o.getStatus() != OrderStatus.PARTIALLY_DISPATCHED && o.getStatus() != OrderStatus.DISPATCHED
                    && o.getStatus() != OrderStatus.DELIVERED) {
                transition(o, OrderStatus.PARTIALLY_DISPATCHED, Actor.SYSTEM, "One or more shipments dispatched");
            }
            if (o.getDispatchedAt() == null) {
                o.setDispatchedAt(at);
                metrics.orderToFulfill(o, at);
            }
            return;
        }

        if (target == OrderStatus.SHIPPED) {
            if (o.getStatus() != OrderStatus.SHIPPED && o.getStatus() != OrderStatus.DISPATCHED
                    && o.getStatus() != OrderStatus.PARTIALLY_DISPATCHED && o.getStatus() != OrderStatus.DELIVERED) {
                transition(o, OrderStatus.SHIPPED, Actor.SYSTEM, "All shipments shipped");
            }
            if (o.getDispatchedAt() == null) {
                o.setDispatchedAt(at);
                metrics.orderToFulfill(o, at);
            }
            return;
        }

        if (target == OrderStatus.PARTIALLY_SHIPPED) {
            if (o.getStatus() != OrderStatus.PARTIALLY_SHIPPED && o.getStatus() != OrderStatus.SHIPPED
                    && o.getStatus() != OrderStatus.DISPATCHED && o.getStatus() != OrderStatus.PARTIALLY_DISPATCHED
                    && o.getStatus() != OrderStatus.DELIVERED) {
                transition(o, OrderStatus.PARTIALLY_SHIPPED, Actor.SYSTEM, "One or more shipments shipped");
            }
            if (o.getDispatchedAt() == null) {
                o.setDispatchedAt(at);
                metrics.orderToFulfill(o, at);
            }
        }
    }

    // ------------------------------------------------------------------
    // compensation
    public void compensate(Order o, OrderSaga s, String reason, Actor actor) {
        if (o.getStatus() == OrderStatus.CANCELLED || o.getStatus() == OrderStatus.DELIVERED)
            return;
        s.setStep(SagaStep.COMPENSATING);
        s.setCompensationReason(reason.length() > 255 ? reason.substring(0, 255) : reason);
        List<Shipment> ships = shipments.findByOrderId(o.getId());
        if (s.isInventoryReserved()) {
            send(o, KafkaTopics.OMS_WMS_INVENTORY_RELEASE, "InventoryReleaseCommand",
                    new Events.InventoryReleaseCommand(
                            o.getId(), o.getTenantId(), reason, allocations(
                                    o, ships.stream().filter(x -> x.getStatus() != ShipmentStatus.CANCELLED).toList())));
            s.setInventoryReserved(false);
        }
        if (s.isPaymentCaptured()) {
            refund(o, "ORDER_CANCEL", o.getId(), o.payable(), o.getWalletAmount(), reason);
            s.setPaymentCaptured(false);
        }
        for (Shipment sh : ships) {
            if (sh.getStatus() == ShipmentStatus.CANCELLED || sh.getStatus() == ShipmentStatus.DELIVERED
                    || sh.getStatus() == ShipmentStatus.DISPATCHED)
                continue;
            if (sh.getStatus() != ShipmentStatus.PLANNED) {
                send(o, KafkaTopics.OMS_LOGISTICS_SHIPMENT_CANCEL, "ShipmentCancelCommand",
                        new Events.ShipmentCancelCommand(o.getId(), sh.getId(), reason));
            }
            shipmentTransition(o, sh, ShipmentStatus.CANCELLED, actor, reason);
        }
        o.setCancelReason(o.getCancelReason() == null ? s.getCompensationReason() : o.getCancelReason());
        o.setCancelledBy(actor.id());
        transition(o, OrderStatus.CANCELLED, actor, reason);
        s.setStep(SagaStep.COMPENSATED);
    }

    public void refund(Order o, String refType, UUID refId, BigDecimal amount, BigDecimal wallet, String reason) {
        if (amount.signum() <= 0 && wallet.signum() <= 0)
            return;
        send(o, KafkaTopics.OMS_PAYMENT_PAYMENT_REFUND, "PaymentRefundCommand",
                new Events.PaymentRefundCommand(o.getId(), o.getTenantId(), refType, refId, o.getCurrency(), amount,
                        wallet, reason));
    }

    private void releaseAll(Order o, String reason) {
        List<Shipment> ships = shipments.findByOrderId(o.getId());
        send(o, KafkaTopics.OMS_WMS_INVENTORY_RELEASE, "InventoryReleaseCommand",
                new Events.InventoryReleaseCommand(o.getId(), o.getTenantId(), reason, allocations(o, ships)));
    }

    @Transactional
    public void onShipmentItemPacked(Events.ShipmentItemPackedEvent e) {
        Shipment sh = shipments.findById(e.shipmentId()).orElseThrow(() -> new NotFoundException("Shipment not found: " + e.shipmentId()));
        Order o = lock(sh.getOrderId());

        ShipmentItem line = sh.getItems().stream()
            .filter(si -> si.getId().equals(e.shipmentLineId()))
            .findFirst()
            .orElse(null);
        if (line == null) return;

        if (line.getPackedQty() >= line.getQuantity()) {
            line.setStatus(ShipmentLineStatus.PACKED);
        } else {
            line.setStatus(line.getPackedQty() > 0 ? ShipmentLineStatus.PICK_COMPLETED : ShipmentLineStatus.PICK_IN_PROGRESS);
        }

        shipments.saveAndFlush(sh);

        OrderItem orderItem = o.getItems().stream()
            .filter(i -> i.getId().equals(e.orderItemId()))
            .findFirst()
            .orElse(null);
        if (orderItem != null) {
            orderItem.setPackedQty(Math.max(orderItem.getPackedQty(), e.totalPacked()));
            orderItem.setStatus(orderItem.getPackedQty() >= orderItem.getAllocatedQty() ? OrderLineStatus.PACKED : OrderLineStatus.PICK_COMPLETED);
        }

        orders.saveAndFlush(o);
        
        if (sh.getItems().stream().allMatch(si -> si.getPackedQty() >= si.getQuantity())) {
            shipmentTransition(o, sh, ShipmentStatus.PACKED, Actor.SYSTEM, "all shipment lines packed");
            audit.record(o.getTenantId(), "SHIPMENT", sh.getId().toString(), "SHIPMENT_PACKED", sh.getStatus().name(), ShipmentStatus.PACKED.name(), Actor.SYSTEM, "warehouseId=" + sh.getWarehouseId());
            send(o, KafkaTopics.OMS_NOTIFICATION_ORDER_LIFECYCLE, "ShipmentPackedEvent", new Events.ShipmentPackedEvent(o.getId(), sh.getId(), sh.getWarehouseId(), Instant.now()));
        }

        List<Shipment> active = shipments.findByOrderId(o.getId()).stream().filter(s -> s.getStatus() != ShipmentStatus.CANCELLED).toList();
        boolean allPacked = !active.isEmpty() && active.stream().allMatch(s -> s.getStatus() == ShipmentStatus.PACKED);
        if (allPacked && o.getStatus() != OrderStatus.CANCELLED && o.getStatus() != OrderStatus.DELIVERED) {
            if (o.getStatus() != OrderStatus.PACKED) {
                transition(o, OrderStatus.PACKED, Actor.SYSTEM, "all shipments packed");
            }
            transition(o, OrderStatus.READY_TO_SHIP, Actor.SYSTEM, "all shipments packed and ready for dispatch");
            send(o, KafkaTopics.OMS_NOTIFICATION_ORDER_LIFECYCLE, "OrderReadyToShipEvent",
                new Events.OrderReadyToShipEvent(o.getId(), o.getOrderNumber(), o.getTenantId(), Instant.now()));
        }
    }
}
