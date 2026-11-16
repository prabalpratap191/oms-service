package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "rmas") @Getter @Setter @NoArgsConstructor
public class Rma {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    @Version private Long version;
    private String tenantId;
    private String rmaNumber;
    private UUID orderId;
    @Enumerated(EnumType.STRING) private RmaType rmaType;
    @Enumerated(EnumType.STRING) private RmaStatus status;
    private String reason;
    private BigDecimal refundTotal;
    private UUID childOrderId;
    private String createdBy;
    private Instant createdAt;
    private Instant updatedAt;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "rma_id", nullable = false)
    private List<RmaItem> items = new ArrayList<>();

    @PrePersist void onCreate() { createdAt = Instant.now(); updatedAt = createdAt; }
    @PreUpdate void onUpdate() { updatedAt = Instant.now(); }
}
