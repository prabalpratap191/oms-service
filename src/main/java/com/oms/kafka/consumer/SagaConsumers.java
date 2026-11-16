package com.oms.kafka.consumer;

import com.oms.kafka.KafkaGroups;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.service.SagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Responses from WMS / payment / logistics that drive the order saga. */
@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class SagaConsumers {
    private final InboundSupport in;
    private final SagaOrchestrator saga;

    @KafkaListener(topics = KafkaTopics.WMS_OMS_INVENTORY_CHECK_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onInventoryCheck(ConsumerRecord<String, String> r) {
        in.handle(r, Events.InventoryCheckResult.class, e -> saga.onInventoryCheckResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.WMS_OMS_INVENTORY_RESERVE_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onInventoryReserve(ConsumerRecord<String, String> r) {
        in.handle(r, Events.InventoryReserveResult.class, e -> saga.onInventoryReserveResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.WMS_OMS_INVENTORY_RELEASE_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onInventoryRelease(ConsumerRecord<String, String> r) {
        in.handle(r, Events.InventoryReleaseResult.class, e -> log.info("Inventory release for order {} success={}", e.payload().orderId(), e.payload().success()));
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_OMS_PAYMENT_CAPTURE_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onPaymentCapture(ConsumerRecord<String, String> r) {
        in.handle(r, Events.PaymentCaptureResult.class, e -> saga.onPaymentCaptureResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_OMS_PAYMENT_RECONCILE_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onPaymentReconcile(ConsumerRecord<String, String> r) {
        in.handle(r, Events.PaymentReconcileResult.class, e -> saga.onPaymentReconcileResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.LOGISTICS_OMS_SHIPMENT_CREATE_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onShipmentCreate(ConsumerRecord<String, String> r) {
        in.handle(r, Events.ShipmentCreateResult.class, e -> saga.onShipmentCreateResult(e.payload()));
    }

    @KafkaListener(topics = KafkaTopics.LOGISTICS_OMS_SHIPMENT_CANCEL_RESULT, groupId = KafkaGroups.OMS_SAGA)
    public void onShipmentCancel(ConsumerRecord<String, String> r) {
        in.handle(r, Events.ShipmentCancelResult.class, e -> log.info("Shipment {} cancel success={}", e.payload().shipmentId(), e.payload().success()));
    }
}
