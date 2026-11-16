package com.oms.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.oms.domain.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record CreateOrderRequest(
    @NotBlank String tenantId, String parentOrgId, String subOrgId,
    @NotBlank String customerId, String customerName,
    @NotNull OrderChannel channel, @NotNull OrderType orderType,
    @NotBlank String cartToken, String externalOrderRef,
    @NotBlank @Size(min = 3, max = 3) String currency, String regionCode,
    @NotNull @Valid Address shippingAddress,
    Instant deliverySlotStart, Instant deliverySlotEnd, Instant pickingCutoffAt,
    @PositiveOrZero BigDecimal deliveryFee, @PositiveOrZero BigDecimal walletAmount,
    PaymentDetails paymentDetails, Priority priority,
    @NotEmpty @Valid List<Line> items) {

    public record PaymentDetails(String paymentMethod, String paymentProvider, String paymentStatus,
                                String cardLast4, String utrReference, String gatewayTransactionId,
                                BigDecimal amountAuthorized, BigDecimal amountCaptured, BigDecimal amountRefunded,
                                BigDecimal amountNet) {}

    public record Line(@NotBlank String sku, String productName, @NotBlank String category, String hsnCode,
                       @Min(1) int quantity, @NotNull @PositiveOrZero BigDecimal unitPrice,
                       @PositiveOrZero BigDecimal discountAmount, TaxType taxType, @PositiveOrZero BigDecimal taxRate,
                      @JsonFormat(shape = JsonFormat.Shape.STRING)
    String warehouseId
) {
    // Overloaded canonical-like constructor if you also construct it manually in Java code using Long
    public Line(String sku, String productName, String category, String hsnCode,
                int quantity, BigDecimal unitPrice, BigDecimal discountAmount,
                TaxType taxType, BigDecimal taxRate, Long warehouseId) {
        this(sku, productName, category, hsnCode, quantity, unitPrice, discountAmount,
             taxType, taxRate, warehouseId == null ? null : String.valueOf(warehouseId));
        }
    }
}
