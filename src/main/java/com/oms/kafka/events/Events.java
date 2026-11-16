package com.oms.kafka.events;

import com.oms.domain.Address;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Payload contracts (schemaVersion 1). Message key is always the orderId / rmaId for per-order ordering. */
public final class Events {
    private Events() {}

    public record Line(UUID orderItemId, String sku, String category, int quantity) {}
    public record ShipmentAllocation(UUID shipmentId, String nodeId, List<Line> items) {}
    public record ReturnLine(UUID orderItemId, String sku, int quantity, String destinationNodeId) {}

    public record ShipmentItemPackedEvent(UUID orderId, UUID shipmentId, UUID shipmentLineId, UUID orderItemId,
                                         String sku, String warehouseId, int quantityPacked, int totalPacked, int totalAllocated,
                                         Instant occurredAt) {}
    public record ShipmentPackedEvent(UUID orderId, UUID shipmentId, String warehouseId, Instant occurredAt) {}
    public record OrderReadyToShipEvent(UUID orderId, String orderNumber, String tenantId, Instant occurredAt) {}

    // ---------------- commands / events published by OMS ----------------
    public record InventoryCheckCommand(UUID orderId, String tenantId, String regionCode, List<String> excludedNodes, List<Line> items) {}
    public record InventoryReserveCommand(UUID orderId, String tenantId, List<ShipmentAllocation> shipments) {}
    public record InventoryReleaseCommand(UUID orderId, String tenantId, String reason, List<ShipmentAllocation> shipments) {}
    public record PaymentCaptureCommand(UUID orderId, String tenantId, String customerId, String currency, BigDecimal amount, BigDecimal walletAmount) {}
    public record PaymentRefundCommand(UUID orderId, String tenantId, String referenceType, UUID referenceId, String currency,
                                       BigDecimal amount, BigDecimal walletAmount, String reason) {}
    public record PaymentReconcileCommand(UUID orderId, String tenantId, String currency, BigDecimal expectedAmount) {}
    public record ShipmentCreateCommand(UUID orderId, UUID shipmentId, String tenantId, String fulfillmentShopId, String deliveryClass,
                                        Instant slaWindowEnd, String priority, Address shippingAddress, List<Line> items) {}
    public record ShipmentUpdateCommand(UUID orderId, UUID shipmentId, Address shippingAddress, Instant deliverySlotStart, Instant deliverySlotEnd) {}
    public record ShipmentCancelCommand(UUID orderId, UUID shipmentId, String reason) {}
    public record ReturnPickupCommand(UUID rmaId, UUID orderId, String tenantId, Address pickupAddress, List<ReturnLine> items) {}
    public record ReturnInspectionCommand(UUID rmaId, UUID orderId, String tenantId, List<ReturnLine> items) {}
    public record CreditMemoLine(UUID orderItemId, String sku, int quantity, BigDecimal itemAmount, BigDecimal discountApportioned, BigDecimal taxAmount) {}
    public record CreditMemoEvent(UUID creditMemoId, UUID orderId, UUID rmaId, String tenantId, String currency, List<CreditMemoLine> lines,
                                  BigDecimal itemsAmount, BigDecimal taxAmount, BigDecimal deliveryFeeRefund,
                                  BigDecimal walletRefund, BigDecimal originalPaymentRefund, BigDecimal totalRefund) {}
    public record OrderLifecycleEvent(UUID orderId, String orderNumber, String tenantId, String customerId,
                                      String fromStatus, String toStatus, String actorType, String actorId, String reason) {}
    public record OrderIngestResult(String externalOrderRef, UUID orderId, String orderNumber, boolean success, String error) {}

    // ---------------- responses consumed by OMS ----------------
    public record Allocation(UUID orderItemId, String sku, String nodeId, int quantity) {}
    public record Unfulfilled(UUID orderItemId, String sku, String nodeId, int quantity, String reason) {}
    public record InventoryCheckResult(UUID orderId, List<Allocation> allocated, List<Unfulfilled> unfulfilled) {}
    public record InventoryReserveResult(UUID orderId, boolean success, String reason) {}
    public record InventoryReleaseResult(UUID orderId, boolean success) {}
    public record PaymentCaptureResult(UUID orderId, boolean success, String paymentId, String reason) {}
    public record PaymentRefundResult(UUID orderId, String referenceType, UUID referenceId, boolean success, String refundId, String reason) {}
    public record PaymentReconcileResult(UUID orderId, String status, BigDecimal capturedAmount) {}   // CAPTURED | NOT_CAPTURED
    public record ShipmentCreateResult(UUID orderId, UUID shipmentId, boolean success, String courier, String trackingNumber, String reason) {}
    public record ShipmentStatusUpdate(UUID orderId, UUID shipmentId, String status, Instant occurredAt) {}  // PACKED | DISPATCHED | DELIVERED
    public record ShipmentCancelResult(UUID orderId, UUID shipmentId, boolean success) {}
    public record ReturnPickupResult(UUID rmaId, UUID orderId, String status, Instant pickupAt) {}          // SCHEDULED | DELIVERED_TO_HUB
    public record InspectionLine(UUID orderItemId, String sku, int acceptedQty, int rejectedQty) {}
    public record ReturnInspectionResult(UUID rmaId, UUID orderId, List<InspectionLine> lines) {}
    public record CreditMemoAck(UUID creditMemoId, UUID orderId, UUID rmaId, boolean success) {}
    public record ShipmentPickPackCommand (UUID orderId, UUID shipmentId, String tenantId, String fulfillmentShopId, List<Line> list){}// Shipment mark for pick and pack command
    
}
