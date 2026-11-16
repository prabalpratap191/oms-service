package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "outbox_events") @Getter @Setter @NoArgsConstructor
public class OutboxEvent {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String topic;
    private String messageKey;
    private String tenantId;
    private String payload;
    @Enumerated(EnumType.STRING) private OutboxStatus status = OutboxStatus.PENDING;
    private int retryCount;
    private String lastError;
    private Instant createdAt = Instant.now();
    private Instant publishedAt;
}
