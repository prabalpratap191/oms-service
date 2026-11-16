package com.oms.service;

import com.oms.config.OmsProperties;
import com.oms.domain.OutboxEvent;
import com.oms.domain.OutboxStatus;
import com.oms.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Relays outbox rows to Kafka. Rows are claimed with FOR UPDATE SKIP LOCKED so several pods can run safely.
 * A row gets max-attempts tries (default 2 = first try + 1 retry); after that it is parked in failed_events.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {
    private final OutboxEventRepository repo;
    private final KafkaTemplate<String, String> kafka;
    private final FailedEventService failedEvents;
    private final OmsProperties props;
    private final TransactionTemplate tx;

    @Scheduled(fixedDelayString = "${oms.outbox.poll-interval-ms}")
    public void poll() {
        try {
            tx.executeWithoutResult(st -> {
                List<OutboxEvent> batch = repo.lockPending(props.outbox().batchSize());
                Set<String> blockedAggregates = new HashSet<>();   // keep per-aggregate ordering after a failure
                for (OutboxEvent e : batch) {
                    if (blockedAggregates.contains(e.getAggregateId())) continue;
                    if (!publish(e)) blockedAggregates.add(e.getAggregateId());
                }
            });
        } catch (Exception ex) {
            log.error("Outbox poll failed", ex);
        }
    }

    private boolean publish(OutboxEvent e) {
        try {
            log.info("Publishing outbox message id={} topic={} key={} aggregateId={} status={}",
                e.getId(), e.getTopic(), e.getMessageKey(), e.getAggregateId(), e.getStatus());
            kafka.send(e.getTopic(), e.getMessageKey(), e.getPayload()).get(5, TimeUnit.SECONDS);
            e.setStatus(OutboxStatus.PUBLISHED);
            e.setPublishedAt(Instant.now());
            log.info("Published outbox message id={} topic={}", e.getId(), e.getTopic());
            return true;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            e.setRetryCount(e.getRetryCount() + 1);
            e.setLastError(String.valueOf(ex.getMessage()));
            log.warn("Outbox publish failed (attempt {}/{}) id={} topic={}: {}", e.getRetryCount(),
                props.outbox().maxAttempts(), e.getId(), e.getTopic(), ex.toString());
            if (e.getRetryCount() >= props.outbox().maxAttempts()) {
                e.setStatus(OutboxStatus.FAILED);
                failedEvents.recordOutboxFailure(e, ex);
            }
            return false;
        }
    }
}
