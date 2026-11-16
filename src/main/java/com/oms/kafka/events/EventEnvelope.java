package com.oms.kafka.events;

import java.time.Instant;
import java.util.UUID;

/** Common envelope for every OMS message. eventId is the consumer de-duplication key. */
public record EventEnvelope<T>(UUID eventId, String eventType, String tenantId, String aggregateType,
                               String aggregateId, String correlationId, Instant occurredAt, int schemaVersion, T payload) {}
