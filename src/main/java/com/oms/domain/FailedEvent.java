package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "failed_events") @Getter @Setter @NoArgsConstructor
public class FailedEvent {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    @Enumerated(EnumType.STRING) private FailureSource source;
    private String topic;
    private Integer partitionNo;
    private Long offsetNo;
    private String messageKey;
    private String payload;
    private UUID outboxEventId;
    private String errorMessage;
    @Column(length = 8000) private String stackTrace;
    @Enumerated(EnumType.STRING) private FailedEventStatus status = FailedEventStatus.FAILED;
    private int retryCount;
    private Instant createdAt = Instant.now();
    private Instant lastRetriedAt;
}
