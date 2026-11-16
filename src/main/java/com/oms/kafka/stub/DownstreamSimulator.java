package com.oms.kafka.stub;

import com.oms.config.OmsProperties;
import com.oms.kafka.KafkaGroups;
import com.oms.kafka.KafkaPublisher;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.EventCodec;
import com.oms.kafka.events.EventEnvelope;
import com.oms.kafka.events.Events;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;

/**
 * Stand-in for WMS / Payment / Logistics / Finance until the real systems exist: consumes every OMS command topic and
 * answers with a SUCCESS event on the matching response topic. Switch off with oms.stub.enabled=false.
 *
 * Test knobs: SKU "OOS-*" is out of stock at the primary node (=> dynamic re-sourcing to WH-BLR-ALT);
 *             SKU "NOSTOCK-*" is unavailable everywhere (=> UNFULFILLABLE, order cancelled).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "oms.stub.enabled", havingValue = "true")
public class DownstreamSimulator {
    private static final String ALT_NODE = "WH-BLR-ALT";

    private final EventCodec codec;
    private final KafkaPublisher publisher;
    private final OmsProperties props;

    // ---------------------------------------------------------------- WMS
    @KafkaListener(topics = KafkaTopics.OMS_WMS_INVENTORY_CHECK, groupId = KafkaGroups.STUB_WMS)
    public void inventoryCheck(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.InventoryCheckCommand> e = codec.parse(r.value(), Events.InventoryCheckCommand.class);
        Events.InventoryCheckCommand c = e.payload();
        List<String> excluded = c.excludedNodes() == null ? List.of() : c.excludedNodes();
        List<Events.Allocation> ok = new ArrayList<>();
        List<Events.Unfulfilled> bad = new ArrayList<>();
        for (Events.Line l : c.items()) {
            String primary = "FRESH_FOOD".equalsIgnoreCase(l.category()) || "GROCERY_FRESH".equalsIgnoreCase(l.category()) ? "DS-BLR-01" : "WH-BLR-REGIONAL";
            String node = excluded.contains(primary) ? ALT_NODE : primary;
            if (l.sku().startsWith("NOSTOCK-") || excluded.contains(node) || (l.sku().startsWith("OOS-") && node.equals(primary))) {
                bad.add(new Events.Unfulfilled(l.orderItemId(), l.sku(), node, l.quantity(), "STOCKOUT"));
            } else {
                ok.add(new Events.Allocation(l.orderItemId(), l.sku(), node, l.quantity()));
            }
        }
        reply(KafkaTopics.WMS_OMS_INVENTORY_CHECK_RESULT, "InventoryCheckResult", "Order", c.orderId(), e, new Events.InventoryCheckResult(c.orderId(), ok, bad));
    }

    @KafkaListener(topics = KafkaTopics.OMS_WMS_INVENTORY_RESERVE, groupId = KafkaGroups.STUB_WMS)
    public void inventoryReserve(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.InventoryReserveCommand> e = codec.parse(r.value(), Events.InventoryReserveCommand.class);
        reply(KafkaTopics.WMS_OMS_INVENTORY_RESERVE_RESULT, "InventoryReserveResult", "Order", e.payload().orderId(), e,
            new Events.InventoryReserveResult(e.payload().orderId(), true, null));
    }

    @KafkaListener(topics = KafkaTopics.OMS_WMS_INVENTORY_RELEASE, groupId = KafkaGroups.STUB_WMS)
    public void inventoryRelease(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.InventoryReleaseCommand> e = codec.parse(r.value(), Events.InventoryReleaseCommand.class);
        reply(KafkaTopics.WMS_OMS_INVENTORY_RELEASE_RESULT, "InventoryReleaseResult", "Order", e.payload().orderId(), e,
            new Events.InventoryReleaseResult(e.payload().orderId(), true));
    }

    @KafkaListener(topics = KafkaTopics.OMS_WMS_RETURN_INSPECTION, groupId = KafkaGroups.STUB_WMS)
    public void returnInspection(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.ReturnInspectionCommand> e = codec.parse(r.value(), Events.ReturnInspectionCommand.class);
        List<Events.InspectionLine> lines = e.payload().items().stream()
            .map(i -> new Events.InspectionLine(i.orderItemId(), i.sku(), i.quantity(), 0)).toList();
        reply(KafkaTopics.WMS_OMS_RETURN_INSPECTION_RESULT, "ReturnInspectionResult", "Rma", e.payload().rmaId(), e,
            new Events.ReturnInspectionResult(e.payload().rmaId(), e.payload().orderId(), lines));
    }

     @KafkaListener(topics = KafkaTopics.OMS_PAYMENT_PAYMENT_CAPTURE, groupId = KafkaGroups.STUB_PAYMENT)
    public void paymentCapture(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.PaymentCaptureCommand> e = codec.parse(r.value(), Events.PaymentCaptureCommand.class);
        reply(KafkaTopics.PAYMENT_OMS_PAYMENT_CAPTURE_RESULT, "PaymentCaptureResult", "Order", e.payload().orderId(), e,
            new Events.PaymentCaptureResult(e.payload().orderId(), true, "PAY-" + UUID.randomUUID(), null));
    }

    // ---------------------------------------------------------------- Payment
    @KafkaListener(topics = KafkaTopics.OMS_LOGISTICS_SHIPMENT_PICK_PACK, groupId = KafkaGroups.STUB_WMS)
    public void shipmentPickPack(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.ShipmentPickPackCommand> e = codec.parse(r.value(), Events.ShipmentPickPackCommand.class);
        // reply(KafkaTopics.LOGISTICS_OMS_SHIPMENT_PICK_PACK_RESULT, "ShipmentPickPackResult", "Shipment", e.payload().shipmentId(), e,
            // new Events.ShipmentPickPackResult(e.payload().shipmentId(), true, null));
            log.info("Shipment {} are in Ready For Pick status",e.payload().shipmentId());
    }

    @KafkaListener(topics = KafkaTopics.OMS_PAYMENT_PAYMENT_REFUND, groupId = KafkaGroups.STUB_PAYMENT)
    public void paymentRefund(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.PaymentRefundCommand> e = codec.parse(r.value(), Events.PaymentRefundCommand.class);
        Events.PaymentRefundCommand c = e.payload();
        reply(KafkaTopics.PAYMENT_OMS_PAYMENT_REFUND_RESULT, "PaymentRefundResult", "Order", c.orderId(), e,
            new Events.PaymentRefundResult(c.orderId(), c.referenceType(), c.referenceId(), true, "RFD-" + UUID.randomUUID(), null));
    }

    @KafkaListener(topics = KafkaTopics.OMS_PAYMENT_PAYMENT_RECONCILE, groupId = KafkaGroups.STUB_PAYMENT)
    public void paymentReconcile(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.PaymentReconcileCommand> e = codec.parse(r.value(), Events.PaymentReconcileCommand.class);
        reply(KafkaTopics.PAYMENT_OMS_PAYMENT_RECONCILE_RESULT, "PaymentReconcileResult", "Order", e.payload().orderId(), e,
            new Events.PaymentReconcileResult(e.payload().orderId(), "CAPTURED", e.payload().expectedAmount()));
    }

    // ---------------------------------------------------------------- Logistics
    @KafkaListener(topics = KafkaTopics.OMS_LOGISTICS_SHIPMENT_CREATE, groupId = KafkaGroups.STUB_LOGISTICS)
    public void shipmentCreate(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.ShipmentCreateCommand> e = codec.parse(r.value(), Events.ShipmentCreateCommand.class);
        Events.ShipmentCreateCommand c = e.payload();
        String courier = "INSTANT".equals(c.deliveryClass()) ? "HYPERLOCAL-FLEET" : "TWO_DAY".equals(c.deliveryClass()) ? "BLUEDART" : "DELHIVERY";
        String tracking = "TRK" + Long.toString(Math.abs(c.shipmentId().getMostSignificantBits()), 36).toUpperCase();
        reply(KafkaTopics.LOGISTICS_OMS_SHIPMENT_CREATE_RESULT, "ShipmentCreateResult", "Shipment", c.shipmentId(), e, c.orderId().toString(),
            new Events.ShipmentCreateResult(c.orderId(), c.shipmentId(), true, courier, tracking, null));
        if (props.stub().autoDeliver()) {                               // ordered on the same key => same partition
            for (String st : List.of("PACKED", "DISPATCHED", "DELIVERED")) {
                reply(KafkaTopics.LOGISTICS_OMS_SHIPMENT_STATUS, "ShipmentStatusUpdate", "Shipment", c.shipmentId(), e, c.orderId().toString(),
                    new Events.ShipmentStatusUpdate(c.orderId(), c.shipmentId(), st, Instant.now()));
            }
        }
    }

    @KafkaListener(topics = KafkaTopics.OMS_LOGISTICS_SHIPMENT_UPDATE, groupId = KafkaGroups.STUB_LOGISTICS)
    public void shipmentUpdate(ConsumerRecord<String, String> r) {
        log.info("[stub-logistics] shipmentUpdate {}", r.value());
    }

    @KafkaListener(topics = KafkaTopics.OMS_LOGISTICS_SHIPMENT_CANCEL, groupId = KafkaGroups.STUB_LOGISTICS)
    public void shipmentCancel(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.ShipmentCancelCommand> e = codec.parse(r.value(), Events.ShipmentCancelCommand.class);
        reply(KafkaTopics.LOGISTICS_OMS_SHIPMENT_CANCEL_RESULT, "ShipmentCancelResult", "Shipment", e.payload().shipmentId(), e,
            e.payload().orderId().toString(), new Events.ShipmentCancelResult(e.payload().orderId(), e.payload().shipmentId(), true));
    }

    @KafkaListener(topics = KafkaTopics.OMS_LOGISTICS_RETURN_PICKUP, groupId = KafkaGroups.STUB_LOGISTICS)
    public void returnPickup(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.ReturnPickupCommand> e = codec.parse(r.value(), Events.ReturnPickupCommand.class);
        Events.ReturnPickupCommand c = e.payload();
        reply(KafkaTopics.LOGISTICS_OMS_RETURN_PICKUP_RESULT, "ReturnPickupResult", "Rma", c.rmaId(), e,
            new Events.ReturnPickupResult(c.rmaId(), c.orderId(), "SCHEDULED", Instant.now().plusSeconds(3600)));
        if (props.stub().autoDeliver()) {
            reply(KafkaTopics.LOGISTICS_OMS_RETURN_PICKUP_RESULT, "ReturnPickupResult", "Rma", c.rmaId(), e,
                new Events.ReturnPickupResult(c.rmaId(), c.orderId(), "DELIVERED_TO_HUB", Instant.now()));
        }
    }

    // ---------------------------------------------------------------- Finance
    @KafkaListener(topics = KafkaTopics.OMS_FINANCE_CREDIT_MEMO, groupId = KafkaGroups.STUB_FINANCE)
    public void creditMemo(ConsumerRecord<String, String> r) {
        EventEnvelope<Events.CreditMemoEvent> e = codec.parse(r.value(), Events.CreditMemoEvent.class);
        Events.CreditMemoEvent c = e.payload();
        reply(KafkaTopics.FINANCE_OMS_CREDIT_MEMO_ACK, "CreditMemoAck", "Rma", c.rmaId(), e, new Events.CreditMemoAck(c.creditMemoId(), c.orderId(), c.rmaId(), true));
    }

    // ---------------------------------------------------------------- helper
    private void reply(String topic, String type, String aggType, UUID aggId, EventEnvelope<?> cause, Object payload) {
        reply(topic, type, aggType, aggId, cause, cause.correlationId(), payload);
    }

    private void reply(String topic, String type, String aggType, UUID aggId, EventEnvelope<?> cause, String correlationId, Object payload) {
        publisher.send(topic, correlationId != null ? correlationId : aggId.toString(), type, aggType, aggId.toString(), cause.tenantId(), correlationId, payload);
    }
}
