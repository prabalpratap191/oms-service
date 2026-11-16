package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

/** Immutable: rows are insert-only (DB trigger blocks UPDATE/DELETE). */
@Entity @Table(name = "audit_logs") @Getter @Setter @NoArgsConstructor
public class AuditLog {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    private String tenantId;
    private String aggregateType;
    private String aggregateId;
    private String action;
    private String fromStatus;
    private String toStatus;
    @Enumerated(EnumType.STRING) private ActorType actorType;
    private String actorId;
    @Column(length = 2000) private String details;
    private Instant createdAt = Instant.now();
}
