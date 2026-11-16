package com.oms.kafka;

import java.util.List;

/** Single source of truth for topic names. Template: sourceSystem.destinationSystem.purpose */
public final class KafkaTopics {
    private KafkaTopics() {}

    // ---- OMS -> others (commands / events)
    public static final String OMS_WMS_INVENTORY_CHECK = "oms.wms.inventoryCheck";
    public static final String OMS_WMS_INVENTORY_RESERVE = "oms.wms.inventoryReserve";
    public static final String OMS_WMS_INVENTORY_RELEASE = "oms.wms.inventoryRelease";
    public static final String OMS_WMS_RETURN_INSPECTION = "oms.wms.returnInspection";
    public static final String OMS_PAYMENT_PAYMENT_CAPTURE = "oms.payment.paymentCapture";
    public static final String OMS_PAYMENT_PAYMENT_REFUND = "oms.payment.paymentRefund";
    public static final String OMS_PAYMENT_PAYMENT_RECONCILE = "oms.payment.paymentReconcile";
    public static final String OMS_LOGISTICS_SHIPMENT_CREATE = "oms.logistics.shipmentCreate";
    public static final String OMS_LOGISTICS_SHIPMENT_UPDATE = "oms.logistics.shipmentUpdate";
    public static final String OMS_LOGISTICS_SHIPMENT_CANCEL = "oms.logistics.shipmentCancel";
    public static final String OMS_LOGISTICS_RETURN_PICKUP = "oms.logistics.returnPickup";
    public static final String OMS_FINANCE_CREDIT_MEMO = "oms.finance.creditMemo";
    public static final String OMS_NOTIFICATION_ORDER_LIFECYCLE = "oms.notification.orderLifecycle";
    public static final String OMS_MARKETPLACE_ORDER_INGEST_RESULT = "oms.marketplace.orderIngestResult";

    // ---- others -> OMS (inbound / success + failure responses)
    public static final String MARKETPLACE_OMS_ORDER_INGEST = "marketplace.oms.orderIngest";
    public static final String WMS_OMS_INVENTORY_CHECK_RESULT = "wms.oms.inventoryCheckResult";
    public static final String WMS_OMS_INVENTORY_RESERVE_RESULT = "wms.oms.inventoryReserveResult";
    public static final String WMS_OMS_INVENTORY_RELEASE_RESULT = "wms.oms.inventoryReleaseResult";
    public static final String WMS_OMS_RETURN_INSPECTION_RESULT = "wms.oms.returnInspectionResult";
    public static final String PAYMENT_OMS_PAYMENT_CAPTURE_RESULT = "payment.oms.paymentCaptureResult";
    public static final String PAYMENT_OMS_PAYMENT_REFUND_RESULT = "payment.oms.paymentRefundResult";
    public static final String PAYMENT_OMS_PAYMENT_RECONCILE_RESULT = "payment.oms.paymentReconcileResult";
    public static final String LOGISTICS_OMS_SHIPMENT_CREATE_RESULT = "logistics.oms.shipmentCreateResult";
    public static final String LOGISTICS_OMS_SHIPMENT_STATUS = "logistics.oms.shipmentStatus";
    public static final String LOGISTICS_OMS_SHIPMENT_CANCEL_RESULT = "logistics.oms.shipmentCancelResult";
    public static final String LOGISTICS_OMS_RETURN_PICKUP_RESULT = "logistics.oms.returnPickupResult";
    public static final String FINANCE_OMS_CREDIT_MEMO_ACK = "finance.oms.creditMemoAck";
    public static final String OMS_LOGISTICS_SHIPMENT_PICK_PACK = "oms.logistics.shipmentPickPack";
    public static final List<String> ALL = List.of(
        OMS_WMS_INVENTORY_CHECK, OMS_WMS_INVENTORY_RESERVE, OMS_WMS_INVENTORY_RELEASE, OMS_WMS_RETURN_INSPECTION,
        OMS_PAYMENT_PAYMENT_CAPTURE, OMS_PAYMENT_PAYMENT_REFUND, OMS_PAYMENT_PAYMENT_RECONCILE,
        OMS_LOGISTICS_SHIPMENT_CREATE, OMS_LOGISTICS_SHIPMENT_UPDATE, OMS_LOGISTICS_SHIPMENT_CANCEL, OMS_LOGISTICS_RETURN_PICKUP,
        OMS_FINANCE_CREDIT_MEMO, OMS_NOTIFICATION_ORDER_LIFECYCLE, OMS_MARKETPLACE_ORDER_INGEST_RESULT,
        MARKETPLACE_OMS_ORDER_INGEST, WMS_OMS_INVENTORY_CHECK_RESULT, WMS_OMS_INVENTORY_RESERVE_RESULT,
        WMS_OMS_INVENTORY_RELEASE_RESULT, WMS_OMS_RETURN_INSPECTION_RESULT,
        PAYMENT_OMS_PAYMENT_CAPTURE_RESULT, PAYMENT_OMS_PAYMENT_REFUND_RESULT, PAYMENT_OMS_PAYMENT_RECONCILE_RESULT,
        LOGISTICS_OMS_SHIPMENT_CREATE_RESULT, LOGISTICS_OMS_SHIPMENT_STATUS, LOGISTICS_OMS_SHIPMENT_CANCEL_RESULT,
        LOGISTICS_OMS_RETURN_PICKUP_RESULT, FINANCE_OMS_CREDIT_MEMO_ACK);
    
}
