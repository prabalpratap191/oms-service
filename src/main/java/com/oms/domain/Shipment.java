package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "shipments") @Getter @Setter @NoArgsConstructor
public class Shipment {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    @Version private Long version;
    private UUID orderId;
    private String tenantId;
    private String warehouseId;
    private String fulfillmentShopId;
    @Enumerated(EnumType.STRING) private DeliveryClass deliveryClass;
    @Enumerated(EnumType.STRING) private ShipmentStatus status;
    private String courier;
    private String trackingNumber;
    @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private Address shippingAddress;
    @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private Address pickupAddress;
    private BigDecimal subtotal;
    private BigDecimal taxAmount;
    private BigDecimal deliveryFee;
    private BigDecimal totalAmount;
    private Instant slaWindowEnd;
    private Instant dispatchedAt;
    private Instant deliveredAt;
    private Instant createdAt;
    private Instant updatedAt;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "shipment_id", nullable = false)
    private List<ShipmentItem> items = new ArrayList<>();

    @PrePersist void onCreate() { createdAt = Instant.now(); updatedAt = createdAt; }
    @PreUpdate void onUpdate() { updatedAt = Instant.now(); }

    public boolean isFullyPacked() {
        return items != null && !items.isEmpty() && items.stream().allMatch(ShipmentItem::isFullyPacked);
    }

    public void addItem(String sku, int qty) {
        for (ShipmentItem si : items) {
            if (si.getSku().equalsIgnoreCase(sku) && Objects.equals(si.getWarehouseId(), warehouseId)) {
                si.setQuantity(si.getQuantity() + qty);
                return;
            }
        }
        ShipmentItem n = new ShipmentItem();
        n.setSku(sku);
        n.setQuantity(qty);
        n.setWarehouseId(warehouseId);
        n.setStatus(ShipmentLineStatus.READY_TO_PICK);
        items.add(n);
    }
}
