package com.oms.service;

import com.oms.domain.OutboxEvent;
import com.oms.kafka.events.EventCodec;
import com.oms.kafka.events.EventEnvelope;
import com.oms.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;

/** Transactional outbox: the event row is written in the SAME transaction as the state change (no dual write). */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {
    private final OutboxEventRepository repo;
    private final EventCodec codec;

    @Transactional(propagation = Propagation.MANDATORY)
    public void enqueue(String topic, String key, String eventType, String aggregateType, String aggregateId,
                        String tenantId, Object payload) {
        EventEnvelope<Object> env = new EventEnvelope<>(UUID.randomUUID(), eventType, tenantId, aggregateType,
            aggregateId, aggregateId, Instant.now(), 1, payload);
        OutboxEvent e = new OutboxEvent();
        e.setAggregateType(aggregateType); e.setAggregateId(aggregateId); e.setEventType(eventType);
        e.setTopic(topic); e.setMessageKey(key); e.setTenantId(tenantId);
        e.setPayload(codec.serialize(env));
        repo.save(e);
    }
}
