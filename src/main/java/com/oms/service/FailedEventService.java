package com.oms.service;

import com.oms.common.InvalidStateException;
import com.oms.common.NotFoundException;
import com.oms.domain.*;
import com.oms.repository.FailedEventRepository;
import com.oms.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.NestedExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Parking table for events that exhausted their retry; supports manual re-drive. */
@Slf4j
@Service
@RequiredArgsConstructor
public class FailedEventService {
    private final FailedEventRepository repo;
    private final OutboxEventRepository outboxRepo;
    private final KafkaTemplate<String, String> kafka;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordConsumerFailure(ConsumerRecord<?, ?> rec, Exception ex) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(ex);
        FailedEvent f = new FailedEvent();
        f.setSource(FailureSource.CONSUMER);
        f.setTopic(rec.topic()); f.setPartitionNo(rec.partition()); f.setOffsetNo(rec.offset());
        f.setMessageKey(rec.key() == null ? null : rec.key().toString());
        f.setPayload(rec.value() == null ? null : rec.value().toString());
        f.setErrorMessage(String.valueOf(root.getMessage()));
        f.setStackTrace(trim(stack(ex)));
        repo.save(f);
        log.error("Event parked in failed_events topic={} partition={} offset={} cause={}", rec.topic(), rec.partition(), rec.offset(), root.toString());
    }

    @Transactional
    public void recordOutboxFailure(OutboxEvent e, Exception ex) {
        FailedEvent f = new FailedEvent();
        f.setSource(FailureSource.OUTBOX);
        f.setTopic(e.getTopic()); f.setMessageKey(e.getMessageKey()); f.setPayload(e.getPayload());
        f.setOutboxEventId(e.getId());
        f.setErrorMessage(String.valueOf(ex.getMessage()));
        f.setStackTrace(trim(stack(ex)));
        repo.save(f);
    }

    @Transactional(readOnly = true)
    public List<FailedEvent> list(FailedEventStatus status) {
        return repo.findByStatusOrderByCreatedAtAsc(status == null ? FailedEventStatus.FAILED : status);
    }

    /** Consumer failures are re-published to their original topic (same eventId, so inbox dedupe stays correct); outbox failures go back to PENDING. */
    @Transactional
    public FailedEvent retry(UUID id) {
        FailedEvent f = repo.findById(id).orElseThrow(() -> new NotFoundException("Failed event not found: " + id));
        if (f.getStatus() == FailedEventStatus.RETRIED) throw new InvalidStateException("Already retried: " + id);
        if (f.getSource() == FailureSource.OUTBOX) {
            OutboxEvent o = outboxRepo.findById(f.getOutboxEventId()).orElseThrow(() -> new NotFoundException("Outbox row missing"));
            o.setStatus(OutboxStatus.PENDING); o.setRetryCount(0);
        } else {
            try {
                kafka.send(f.getTopic(), f.getMessageKey(), f.getPayload()).get(5, TimeUnit.SECONDS);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
                throw new IllegalStateException("Re-publish failed: " + ex.getMessage(), ex);
            }
        }
        f.setStatus(FailedEventStatus.RETRIED);
        f.setRetryCount(f.getRetryCount() + 1);
        f.setLastRetriedAt(Instant.now());
        return f;
    }

    public int retryAll() {
        int n = 0;
        for (FailedEvent f : list(FailedEventStatus.FAILED)) {
            try { retry(f.getId()); n++; } catch (Exception ex) { log.warn("retryAll: {} failed: {}", f.getId(), ex.getMessage()); }
        }
        return n;
    }

    private static String stack(Throwable t) { StringWriter sw = new StringWriter(); t.printStackTrace(new PrintWriter(sw)); return sw.toString(); }

    private static String trim(String s) { return s.length() > 7900 ? s.substring(0, 7900) : s; }
}
