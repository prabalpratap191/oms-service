package com.oms.kafka.consumer;

import com.oms.kafka.KafkaGroups;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.service.SagaOrchestrator;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Carrier / node status stream (PACKED, DISPATCHED, DELIVERED) kept in its own group so it never blocks saga commands. */
@Component
@Transactional
@RequiredArgsConstructor
public class ShipmentConsumers {
    private final InboundSupport in;
    private final SagaOrchestrator saga;

    @KafkaListener(topics = KafkaTopics.LOGISTICS_OMS_SHIPMENT_STATUS, groupId = KafkaGroups.OMS_SHIPMENT)
    public void onShipmentStatus(ConsumerRecord<String, String> r) {
        in.handle(r, Events.ShipmentStatusUpdate.class, e -> saga.onShipmentStatus(e.payload()));
    }
}
