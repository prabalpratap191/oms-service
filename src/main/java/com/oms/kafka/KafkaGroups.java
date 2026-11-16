package com.oms.kafka;

import java.util.List;

/** Single source of truth for consumer group ids. */
public final class KafkaGroups {
    private KafkaGroups() {}

    public static final String OMS_SAGA = "oms-saga-group";
    public static final String OMS_SHIPMENT = "oms-shipment-group";
    public static final String OMS_RMA = "oms-rma-group";
    public static final String OMS_INGEST = "oms-ingest-group";

    // in-process downstream simulators (kafka.stub package)
    public static final String STUB_WMS = "oms-stub-wms-group";
    public static final String STUB_PAYMENT = "oms-stub-payment-group";
    public static final String STUB_LOGISTICS = "oms-stub-logistics-group";
    public static final String STUB_FINANCE = "oms-stub-finance-group";

    public static final List<String> ALL = List.of(OMS_SAGA, OMS_SHIPMENT, OMS_RMA, OMS_INGEST,
        STUB_WMS, STUB_PAYMENT, STUB_LOGISTICS, STUB_FINANCE);
}
