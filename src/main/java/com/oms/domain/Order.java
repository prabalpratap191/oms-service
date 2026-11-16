package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity @Table(name = "orders") @Getter @Setter @NoArgsConstructor
public class Order {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    @Version private Long version;
    private String tenantId;
    private String parentOrgId;
    private String subOrgId;
    private String orderNumber;
    private String customerId;
    @Enumerated(EnumType.STRING) private OrderChannel channel;
    @Enumerated(EnumType.STRING) private OrderType orderType;
    private UUID parentOrderId;
    @Enumerated(EnumType.STRING) private OrderStatus status;
    @Enumerated(EnumType.STRING) private Priority priority;
    private String currency;
    private BigDecimal subtotal;
    private BigDecimal discountTotal;
    private BigDecimal taxTotal;
    private BigDecimal deliveryFee;
    private BigDecimal walletAmount;
    private BigDecimal totalAmount;
    private String idempotencyKey;
    private String externalOrderRef;
    private String cartToken;
    private String regionCode;
    @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private Address shippingAddress;
    private Instant deliverySlotStart;
    private Instant deliverySlotEnd;
    private Instant pickingCutoffAt;
    private boolean onHold;
    private String holdReason;
    private String cancelReason;
    private String cancelledBy;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant dispatchedAt;
    private Instant fulfilledAt;

    private String customerName;
    private String paymentMethod;
    private String paymentProvider;
    private String paymentStatus;
    private String cardLast4;
    private String utrReference;
    private String gatewayTransactionId;
    private BigDecimal amountAuthorized;
    private BigDecimal amountCaptured;
    private BigDecimal amountRefunded;
    private BigDecimal amountNet;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "order_id", nullable = false)
    private List<OrderItem> items = new ArrayList<>();

    @PrePersist void onCreate() { createdAt = Instant.now(); updatedAt = createdAt; }
    @PreUpdate void onUpdate() { updatedAt = Instant.now(); }

    public BigDecimal payable() { return totalAmount.subtract(walletAmount).max(BigDecimal.ZERO); }
    public void recomputeTotals() { totalAmount = subtotal.subtract(discountTotal).add(taxTotal).add(deliveryFee); }
}
