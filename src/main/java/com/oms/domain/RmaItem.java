package com.oms.domain;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;

@Entity @Table(name = "rma_items") @Getter @Setter @NoArgsConstructor
public class RmaItem {
    @Id @GeneratedValue(strategy = GenerationType.UUID) private UUID id;
    private UUID orderItemId;
    private String sku;
    private int quantity;
    private int acceptedQty;
    private int rejectedQty;
    private String returnNodeId;
    private String replacementSku;
    private BigDecimal refundAmount;
}
