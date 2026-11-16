package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "order_items") 
@Getter @Setter @NoArgsConstructor
public class OrderItem {
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
    private int packedQty;
    @Enumerated(EnumType.STRING) private OrderLineStatus status = OrderLineStatus.READY_TO_PICK;
    // Price / tax snapshot taken at the moment of purchase - never mutated by catalog changes.
    private BigDecimal unitPrice;
    private BigDecimal discountAmount;
    @Enumerated(EnumType.STRING) private TaxType taxType;
    private BigDecimal taxRate;
    private BigDecimal cgstAmount;
    private BigDecimal sgstAmount;
    private BigDecimal igstAmount;
    private BigDecimal taxAmount;
    private BigDecimal lineTotal;

    public int effectiveQty() { return orderedQty - cancelledQty; }
    public BigDecimal subtotalFor(int q) { return unitPrice.multiply(BigDecimal.valueOf(q)).setScale(2, RoundingMode.HALF_UP); }
    public BigDecimal discountFor(int q) { return prorate(discountAmount, q); }
    public BigDecimal taxFor(int q) { return prorate(taxAmount, q); }
    public BigDecimal netFor(int q) { return subtotalFor(q).subtract(discountFor(q)).add(taxFor(q)); }
    private BigDecimal prorate(BigDecimal total, int q) {
        if (q == orderedQty) return total;
        return total.multiply(BigDecimal.valueOf(q)).divide(BigDecimal.valueOf(orderedQty), 2, RoundingMode.HALF_UP);
    }
}
