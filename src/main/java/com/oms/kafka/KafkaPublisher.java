package com.oms.kafka;

import com.oms.kafka.events.EventCodec;
import com.oms.kafka.events.EventEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.UUID;

/** Direct (non-outbox) publisher, used by the downstream simulators to answer on response topics. */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final EventCodec codec;

    public void send(String topic, String key, String eventType, String aggregateType, String aggregateId,
                     String tenantId, String correlationId, Object payload) {
        EventEnvelope<Object> env = new EventEnvelope<>(UUID.randomUUID(), eventType, tenantId, aggregateType,
            aggregateId, correlationId, Instant.now(), 1, payload);
        log.info("Publishing Kafka event topic={} key={} eventType={} aggregateType={} aggregateId={} tenantId={} correlationId={}",
            topic, key, eventType, aggregateType, aggregateId, tenantId, correlationId);
        kafka.send(topic, key, codec.serialize(env));
        log.debug("sent {} -> {}", eventType, topic);
    }
}
