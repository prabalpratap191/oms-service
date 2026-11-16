package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "order_saga") @Getter @Setter @NoArgsConstructor
public class OrderSaga {
    @Id private UUID orderId;               // shared primary key with orders.id
    @Version private Long version;
    @Enumerated(EnumType.STRING) private SagaStep step;
    private boolean inventoryReserved;
    private boolean paymentCaptured;
    private int resourcingAttempts;
    private String excludedNodes = "";
    private String compensationReason;
    private Instant updatedAt;

    @PrePersist @PreUpdate void touch() { updatedAt = Instant.now(); }

    public List<String> excludedNodeList() {
        return excludedNodes == null || excludedNodes.isBlank() ? new ArrayList<>() : new ArrayList<>(Arrays.asList(excludedNodes.split(",")));
    }
    public void exclude(String node) {
        if (node == null) return;
        List<String> l = excludedNodeList();
        if (!l.contains(node)) { l.add(node); excludedNodes = String.join(",", l); }
    }
}
