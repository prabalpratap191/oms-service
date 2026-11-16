package com.oms.api.dto;

import com.oms.domain.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class Views {
    private Views() {}

    public record PageResult<T>(List<T> content, long total, int page, int size) {}

    public record ItemView(UUID id, String sku, String productName, String category, String hsnCode, int orderedQty, int allocatedQty,
                           int cancelledQty, int returnedQty, BigDecimal unitPrice, BigDecimal discountAmount, TaxType taxType,
                           BigDecimal taxRate, BigDecimal cgstAmount, BigDecimal sgstAmount, BigDecimal igstAmount,
                           BigDecimal taxAmount, BigDecimal lineTotal) {}

    public record ShipmentItemView(String sku, int quantity, int pickedQuantity, String warehouseId) {}

    public record ShipmentView(UUID id, String fulfillmentShopId, String warehouseId, DeliveryClass deliveryClass, ShipmentStatus status, String courier,
                               String trackingNumber, Address shippingAddress, Address pickupAddress,
                               BigDecimal subtotal, BigDecimal taxAmount, BigDecimal deliveryFee, BigDecimal totalAmount,
                               Instant slaWindowEnd, Instant dispatchedAt, Instant deliveredAt,
                               List<ShipmentItemView> items) {}

    public record OrderView(UUID id, String orderNumber, String tenantId, String parentOrgId, String subOrgId, String customerId,
                            String customerName, OrderChannel channel, OrderType orderType, UUID parentOrderId, OrderStatus status,
                            Priority priority, boolean onHold, String currency, BigDecimal subtotal, BigDecimal discountTotal,
                            BigDecimal taxTotal, BigDecimal deliveryFee, BigDecimal walletAmount, BigDecimal totalAmount,
                            String paymentMethod, String paymentProvider, String paymentStatus, String cardLast4, String utrReference,
                            String gatewayTransactionId, BigDecimal amountAuthorized, BigDecimal amountCaptured,
                            BigDecimal amountRefunded, BigDecimal amountNet, Address shippingAddress,
                            Instant deliverySlotStart, Instant deliverySlotEnd, Instant pickingCutoffAt, Instant createdAt,
                            List<ItemView> items, List<ShipmentView> shipments) {

        public static OrderView from(Order o, List<Shipment> ships) {
            List<ItemView> items = o.getItems().stream().map(i -> new ItemView(i.getId(), i.getSku(), i.getProductName(), i.getCategory(),
                i.getHsnCode(), i.getOrderedQty(), i.getAllocatedQty(), i.getCancelledQty(), i.getReturnedQty(), i.getUnitPrice(),
                i.getDiscountAmount(), i.getTaxType(), i.getTaxRate(), i.getCgstAmount(), i.getSgstAmount(), i.getIgstAmount(),
                i.getTaxAmount(), i.getLineTotal())).toList();
            return new OrderView(o.getId(), o.getOrderNumber(), o.getTenantId(), o.getParentOrgId(), o.getSubOrgId(), o.getCustomerId(),
                o.getCustomerName(), o.getChannel(), o.getOrderType(), o.getParentOrderId(), o.getStatus(), o.getPriority(), o.isOnHold(),
                o.getCurrency(), o.getSubtotal(), o.getDiscountTotal(), o.getTaxTotal(), o.getDeliveryFee(), o.getWalletAmount(), o.getTotalAmount(),
                o.getPaymentMethod(), o.getPaymentProvider(), o.getPaymentStatus(), o.getCardLast4(), o.getUtrReference(), o.getGatewayTransactionId(),
                o.getAmountAuthorized(), o.getAmountCaptured(), o.getAmountRefunded(), o.getAmountNet(), o.getShippingAddress(),
                o.getDeliverySlotStart(), o.getDeliverySlotEnd(), o.getPickingCutoffAt(), o.getCreatedAt(),
                items, ships.stream().map(Views::shipment).toList());
        }
    }

    public static ShipmentView shipment(Shipment s) {
        return new ShipmentView(s.getId(), s.getFulfillmentShopId(), s.getWarehouseId(), s.getDeliveryClass(), s.getStatus(), s.getCourier(), s.getTrackingNumber(),
            s.getShippingAddress(), s.getPickupAddress(), s.getSubtotal(), s.getTaxAmount(), s.getDeliveryFee(), s.getTotalAmount(),
            s.getSlaWindowEnd(), s.getDispatchedAt(), s.getDeliveredAt(),
            s.getItems().stream().map(i -> new ShipmentItemView(i.getSku(), i.getQuantity(), i.getPickedQuantity(), i.getWarehouseId())).toList());
    }

    public record TimelineEntry(Instant at, String aggregateType, String action, String fromStatus, String toStatus,
                                String actor, String details, UUID shipmentId, String trackingNumber, String courier, Instant slaWindowEnd) {}

    public record TimelineView(UUID orderId, String orderNumber, OrderStatus status, List<ShipmentView> shipments, List<TimelineEntry> events) {}

    public record AuditLogView(UUID id, String tenantId, String aggregateType, String aggregateId, String action,
                              String fromStatus, String toStatus, String actorType, String actorId,
                              String details, Instant createdAt) {
        public static AuditLogView from(AuditLog log) {
            return new AuditLogView(log.getId(), log.getTenantId(), log.getAggregateType(), log.getAggregateId(),
                log.getAction(), log.getFromStatus(), log.getToStatus(),
                log.getActorType() == null ? null : log.getActorType().name(), log.getActorId(), log.getDetails(), log.getCreatedAt());
        }
    }

    public record OrderDetailView(UUID id, String orderNumber, String tenantId, String parentOrgId, String subOrgId, String customerId,
                                 String customerName, OrderChannel channel, OrderType orderType, UUID parentOrderId, OrderStatus status,
                                 Priority priority, boolean onHold, String currency, BigDecimal subtotal, BigDecimal discountTotal,
                                 BigDecimal taxTotal, BigDecimal deliveryFee, BigDecimal walletAmount, BigDecimal totalAmount,
                                 String paymentMethod, String paymentProvider, String paymentStatus, String cardLast4, String utrReference,
                                 String gatewayTransactionId, BigDecimal amountAuthorized, BigDecimal amountCaptured,
                                 BigDecimal amountRefunded, BigDecimal amountNet, Address shippingAddress,
                                 Instant deliverySlotStart, Instant deliverySlotEnd, Instant pickingCutoffAt, Instant createdAt,
                                 List<ItemView> itemLines, List<ShipmentView> shipments, List<ShipmentItemView> shipmentLines,
                                 List<AuditLogView> auditLogs, List<TimelineEntry> orderStatusTimeline) {}

    public record RmaItemView(UUID id, UUID orderItemId, String sku, int quantity, int acceptedQty, int rejectedQty,
                             String returnNodeId, String replacementSku, BigDecimal refundAmount) {
        public static RmaItemView from(RmaItem item) {
            return new RmaItemView(item.getId(), item.getOrderItemId(), item.getSku(), item.getQuantity(), item.getAcceptedQty(),
                item.getRejectedQty(), item.getReturnNodeId(), item.getReplacementSku(), item.getRefundAmount());
        }
    }

    public record RmaView(UUID id, String tenantId, String rmaNumber, UUID orderId, RmaType rmaType, RmaStatus status,
                          String reason, BigDecimal refundTotal, UUID childOrderId, String createdBy,
                          Instant createdAt, Instant updatedAt, List<RmaItemView> items) {
        public static RmaView from(Rma rma) {
            return new RmaView(rma.getId(), rma.getTenantId(), rma.getRmaNumber(), rma.getOrderId(), rma.getRmaType(),
                rma.getStatus(), rma.getReason(), rma.getRefundTotal(), rma.getChildOrderId(), rma.getCreatedBy(),
                rma.getCreatedAt(), rma.getUpdatedAt(), rma.getItems().stream().map(RmaItemView::from).toList());
        }
    }

    public record OrderSourcingPage(List<OrderSourcingRow> rows, long total, int page, int size) {}

    public record OrderSourcingRow(UUID id, String orderNumber, String externalRef, String customer, String amount,
                                  String deliveryStartDate, String deliveryEndDate,
                                  String orderStatus, String paymentMethod, String orderType,
                                  String node, String splits, String sla) {
        public static OrderSourcingRow from(Order o) {
            String externalRef = o.getExternalOrderRef() != null ? o.getExternalOrderRef() : "externalRef_Dummy";
            String customer = o.getCustomerName() != null ? o.getCustomerName() : "customer_Dummy";
            String amount = o.getTotalAmount() != null ? o.getTotalAmount().toPlainString() : "amount_Dummy";
            String deliveryStartDate = o.getDeliverySlotStart() != null ? o.getDeliverySlotStart().toString() : "deliveryStartDate_Dummy";
            String deliveryEndDate = o.getDeliverySlotEnd() != null ? o.getDeliverySlotEnd().toString() : "deliveryEndDate_Dummy";
            String orderStatus = o.getStatus() != null ? o.getStatus().name() : "orderStatus_Dummy";
            String paymentMethod = o.getPaymentMethod() != null ? o.getPaymentMethod() : "paymentMethod_Dummy";
            String orderType = o.getOrderType() != null ? o.getOrderType().name() : "orderType_Dummy";
            String node = o.getShippingAddress() != null && o.getShippingAddress().city() != null ? o.getShippingAddress().city() : "node_Dummy";
            String splits = "1";
            String sla = "N/A";
            return new OrderSourcingRow(o.getId(), o.getOrderNumber() != null ? o.getOrderNumber() : "orderNumber_Dummy",
                externalRef, customer, amount, deliveryStartDate, deliveryEndDate, orderStatus, paymentMethod, orderType,
                node, splits, sla);
        }
    }

    public record ShipmentLineView(UUID shipmentLineId, String sku, String title, int orderedQty, int packedQty, int allocatedQty, String status, String warehouseId) {}

    public record ShipmentViewForPacking(UUID shipmentId, String warehouseId, ShipmentStatus status,
         String fulfillmentShopId, Address shippingAddress,String amount, String deliveryFee, String taxAmount, String totalAmount, Instant slaWindowEnd, Instant dispatchedAt, Instant deliveredAt,
                                        List<ShipmentLineView> shipmentLines) {}

    public record ShipmentPage(List<ShipmentViewForPacking> content, long total, int page, int size) {}
}
