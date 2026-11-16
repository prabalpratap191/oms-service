package com.oms.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "oms")
public record OmsProperties(Kafka kafka, Outbox outbox, Sourcing sourcing, Idempotency idempotency, Rma rma, Stub stub) {
    public record Kafka(int partitions, int replicas) {}
    public record Outbox(long pollIntervalMs, int batchSize, int maxAttempts) {}
    public record Sourcing(int maxResourcingAttempts) {}
    public record Idempotency(long lockTtlSeconds) {}
    public record Rma(String reverseHub, int returnWindowDays) {}
    public record Stub(boolean enabled, boolean autoDeliver) {}
}
