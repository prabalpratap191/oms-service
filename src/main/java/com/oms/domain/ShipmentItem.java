package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "shipment_items") @Getter @Setter @NoArgsConstructor
public class ShipmentItem {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    private String sku;
    private String productName;
    private String category;
    private String hsnCode;
    private String warehouseId;
    private int orderedQty;
    private int allocatedQty;
    private int cancelledQty;
    private int returnedQty;
    private int quantity;
    private int pickedQuantity;
    private int packedQty;
    private BigDecimal unitPrice;
    private BigDecimal discountAmount;
    @Enumerated(EnumType.STRING) private TaxType taxType;
    private BigDecimal taxRate;
    private BigDecimal cgstAmount;
    private BigDecimal sgstAmount;
    private BigDecimal igstAmount;
    private BigDecimal taxAmount;
    private BigDecimal lineTotal;
    @Enumerated(EnumType.STRING) private ShipmentLineStatus status = ShipmentLineStatus.READY_TO_PICK;

    public boolean isFullyPacked() { return packedQty >= quantity; }

    public void pack(int qty) {
        if (qty <= 0) return;
        int remaining = quantity - packedQty;
        if (qty > remaining) throw new IllegalArgumentException("Packing quantity exceeds remaining available qty");
        this.packedQty += qty;
        this.status = packedQty >= quantity ? ShipmentLineStatus.PACKED : ShipmentLineStatus.PICK_COMPLETED;
    }
}
