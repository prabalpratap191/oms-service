-- OMS schema (PostgreSQL 14+). Flyway is the single source of truth.
CREATE SEQUENCE IF NOT EXISTS order_number_seq START 1;
CREATE SEQUENCE IF NOT EXISTS rma_number_seq START 1;

CREATE TABLE IF NOT EXISTS orders (
    id                   uuid PRIMARY KEY,
    version              bigint        NOT NULL DEFAULT 0,
    tenant_id            varchar(64)   NOT NULL,
    parent_org_id        varchar(64),
    sub_org_id           varchar(64),
    order_number         varchar(40)   NOT NULL UNIQUE,
    customer_id          varchar(64)   NOT NULL,
    customer_name        varchar(128),
    channel              varchar(32)   NOT NULL,
    order_type           varchar(16)   NOT NULL DEFAULT 'STANDARD',
    parent_order_id      uuid REFERENCES orders (id),
    status               varchar(24)   NOT NULL,
    priority             varchar(16)   NOT NULL DEFAULT 'NORMAL',
    currency             varchar(3)    NOT NULL,
    subtotal             numeric(18,2) NOT NULL,
    discount_total       numeric(18,2) NOT NULL DEFAULT 0,
    tax_total            numeric(18,2) NOT NULL DEFAULT 0,
    delivery_fee         numeric(18,2) NOT NULL DEFAULT 0,
    wallet_amount        numeric(18,2) NOT NULL DEFAULT 0,
    total_amount         numeric(18,2) NOT NULL,
    payment_method       varchar(32),
    payment_provider     varchar(64),
    payment_status       varchar(32),
    card_last4           varchar(8),
    utr_reference        varchar(64),
    gateway_transaction_id varchar(128),
    amount_authorized    numeric(18,2),
    amount_captured      numeric(18,2),
    amount_refunded      numeric(18,2),
    amount_net           numeric(18,2),
    idempotency_key      varchar(64)   NOT NULL UNIQUE,
    external_order_ref   varchar(128),
    cart_token           varchar(128),
    region_code          varchar(32),
    shipping_address     jsonb         NOT NULL,
    delivery_slot_start  timestamptz,
    delivery_slot_end    timestamptz,
    picking_cutoff_at    timestamptz,
    on_hold              boolean       NOT NULL DEFAULT false,
    hold_reason          varchar(255),
    cancel_reason        varchar(255),
    cancelled_by         varchar(64),
    created_at           timestamptz   NOT NULL,
    updated_at           timestamptz   NOT NULL,
    dispatched_at        timestamptz,
    fulfilled_at         timestamptz,
    CONSTRAINT chk_order_status CHECK (status IN ('ORDER_CREATED','PENDING','SOURCED','RESERVED','SPLIT_PROCESSING','READY_TO_PICK','PICK_IN_PROGRESS','PICK_COMPLETED','PACK_IN_PROGRESS','PACKED','READY_TO_SHIP','PARTIALLY_SHIPPED','SHIPPED','PARTIALLY_DISPATCHED','DISPATCHED','PARTIALLY_DELIVERED','DELIVERED','FULFILLED','CANCELLED'))
);
CREATE INDEX IF NOT EXISTS idx_orders_tenant_customer ON orders (tenant_id, customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_tenant_org      ON orders (tenant_id, parent_org_id, sub_org_id);
CREATE INDEX IF NOT EXISTS idx_orders_status          ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_parent          ON orders (parent_order_id);
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_external_ref ON orders (tenant_id, channel, external_order_ref) WHERE external_order_ref IS NOT NULL;

CREATE TABLE IF NOT EXISTS order_items (
    id               uuid PRIMARY KEY,
    order_id         uuid          NOT NULL REFERENCES orders (id),
    sku              varchar(64)   NOT NULL,
    warehouse_id varchar(64) NULL,
    product_name     varchar(255),
    category         varchar(64),
    hsn_code         varchar(16),
    ordered_qty      integer       NOT NULL CHECK (ordered_qty > 0),
    allocated_qty    integer       NOT NULL DEFAULT 0,
    cancelled_qty    integer       NOT NULL DEFAULT 0,
    returned_qty     integer       NOT NULL DEFAULT 0,
    packed_qty       integer       NOT NULL DEFAULT 0,
    status           varchar(24)   NOT NULL DEFAULT 'CREATED',
    unit_price       numeric(18,2) NOT NULL,
    discount_amount  numeric(18,2) NOT NULL DEFAULT 0,
    tax_type         varchar(16)   NOT NULL,
    tax_rate         numeric(6,3)  NOT NULL DEFAULT 0,
    cgst_amount      numeric(18,2) NOT NULL DEFAULT 0,
    sgst_amount      numeric(18,2) NOT NULL DEFAULT 0,
    igst_amount      numeric(18,2) NOT NULL DEFAULT 0,
    tax_amount       numeric(18,2) NOT NULL DEFAULT 0,
    line_total       numeric(18,2) NOT NULL,
    CONSTRAINT chk_order_item_status CHECK (status IN ('CREATED','READY_TO_PICK','PICK_IN_PROGRESS','PICKED','PICK_COMPLETED','PACK_IN_PROGRESS','PACKED','CANCELLED'))
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items (order_id);

CREATE TABLE IF NOT EXISTS shipments (
    id                  uuid PRIMARY KEY,
    version             bigint      NOT NULL DEFAULT 0,
    order_id            uuid        NOT NULL REFERENCES orders (id),
    tenant_id           varchar(64) NOT NULL,
    fulfillment_shop_id varchar(64) NOT NULL,
    warehouse_id        varchar(64) NOT NULL,
    delivery_class      varchar(16) NOT NULL,
    status              varchar(24) NOT NULL,
    courier             varchar(64),
    tracking_number     varchar(64),
    shipping_address    jsonb,
    pickup_address      jsonb,
    subtotal            numeric(18,2) NOT NULL DEFAULT 0,
    tax_amount          numeric(18,2) NOT NULL DEFAULT 0,
    delivery_fee        numeric(18,2) NOT NULL DEFAULT 0,
    total_amount        numeric(18,2) NOT NULL DEFAULT 0,
    sla_window_end      timestamptz,
    dispatched_at       timestamptz,
    delivered_at        timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    UNIQUE (order_id, fulfillment_shop_id, delivery_class),
    CONSTRAINT chk_shipment_status CHECK (status IN ('CREATED','PLANNED','READY_TO_PICK','PICK_IN_PROGRESS','PICK_COMPLETED','PACK_IN_PROGRESS','PACKED','READY_TO_SHIP','DISPATCH_REQUESTED','DISPATCHED','PARTIALLY_DELIVERED','DELIVERED','CANCELLED'))
);
CREATE INDEX IF NOT EXISTS idx_shipments_order  ON shipments (order_id);
CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments (status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_shipments_tracking ON shipments (tracking_number) WHERE tracking_number IS NOT NULL;

CREATE TABLE IF NOT EXISTS shipment_items (
    id               uuid PRIMARY KEY,
    shipment_id      uuid        NOT NULL REFERENCES shipments (id),
    sku              varchar(64) NOT NULL,
    product_name     varchar(255),
    category         varchar(64),
    hsn_code         varchar(16),
    warehouse_id     varchar(64) NOT NULL,
    ordered_qty      integer     NOT NULL DEFAULT 0,
    allocated_qty    integer     NOT NULL DEFAULT 0,
    cancelled_qty    integer     NOT NULL DEFAULT 0,
    returned_qty     integer     NOT NULL DEFAULT 0,
    quantity         integer     NOT NULL CHECK (quantity > 0),
    picked_quantity  integer     NOT NULL DEFAULT 0,
    packed_qty       integer     NOT NULL DEFAULT 0,
    unit_price       numeric(18,2),
    discount_amount  numeric(18,2) NOT NULL DEFAULT 0,
    tax_type         varchar(16),
    tax_rate         numeric(6,3) NOT NULL DEFAULT 0,
    cgst_amount      numeric(18,2) NOT NULL DEFAULT 0,
    sgst_amount      numeric(18,2) NOT NULL DEFAULT 0,
    igst_amount      numeric(18,2) NOT NULL DEFAULT 0,
    tax_amount       numeric(18,2) NOT NULL DEFAULT 0,
    line_total       numeric(18,2),
    status           varchar(24) NOT NULL DEFAULT 'READY_TO_PICK',
    CONSTRAINT chk_shipment_item_status CHECK (status IN ('CREATED','PLANNED','READY_TO_PICK','PICK_IN_PROGRESS','PICKED','PICK_COMPLETED','PACK_IN_PROGRESS','PACKED','CANCELLED'))
);
CREATE INDEX IF NOT EXISTS idx_shipment_items_shipment ON shipment_items (shipment_id);
CREATE INDEX IF NOT EXISTS idx_shipment_items_warehouse ON shipment_items (warehouse_id);

CREATE TABLE IF NOT EXISTS order_saga (
    order_id            uuid PRIMARY KEY REFERENCES orders (id),
    version             bigint      NOT NULL DEFAULT 0,
    step                varchar(24) NOT NULL,
    inventory_reserved  boolean     NOT NULL DEFAULT false,
    payment_captured    boolean     NOT NULL DEFAULT false,
    resourcing_attempts integer     NOT NULL DEFAULT 0,
    excluded_nodes      text        NOT NULL DEFAULT '',
    compensation_reason varchar(255),
    updated_at          timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id             uuid PRIMARY KEY,
    aggregate_type varchar(32)  NOT NULL,
    aggregate_id   varchar(64)  NOT NULL,
    event_type     varchar(64)  NOT NULL,
    topic          varchar(128) NOT NULL,
    message_key    varchar(128),
    tenant_id      varchar(64),
    payload        text         NOT NULL,
    status         varchar(16)  NOT NULL DEFAULT 'PENDING',
    retry_count    integer      NOT NULL DEFAULT 0,
    last_error     text,
    created_at     timestamptz  NOT NULL DEFAULT now(),
    published_at   timestamptz
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS failed_events (
    id              uuid PRIMARY KEY,
    source          varchar(16)  NOT NULL,
    topic           varchar(128) NOT NULL,
    partition_no    integer,
    offset_no       bigint,
    message_key     varchar(128),
    payload         text,
    outbox_event_id uuid,
    error_message   text,
    stack_trace     varchar(8000),
    status          varchar(16)  NOT NULL DEFAULT 'FAILED',
    retry_count     integer      NOT NULL DEFAULT 0,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    last_retried_at timestamptz
);
CREATE INDEX IF NOT EXISTS idx_failed_events_status ON failed_events (status, created_at);

CREATE TABLE IF NOT EXISTS processed_messages (
    id           varchar(200) PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id             uuid PRIMARY KEY,
    tenant_id      varchar(64) NOT NULL,
    aggregate_type varchar(32) NOT NULL,
    aggregate_id     varchar(64) NOT NULL,
    action         varchar(64) NOT NULL,
    from_status    varchar(32),
    to_status      varchar(32),
    actor_type     varchar(16) NOT NULL,
    actor_id       varchar(64) NOT NULL,
    details        varchar(2000),
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_aggregate ON audit_logs (tenant_id, aggregate_id, created_at);

CREATE OR REPLACE FUNCTION audit_logs_immutable() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION 'audit_logs is append-only'; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_audit_logs_immutable ON audit_logs;
CREATE TRIGGER trg_audit_logs_immutable BEFORE UPDATE OR DELETE ON audit_logs
    FOR EACH ROW EXECUTE FUNCTION audit_logs_immutable();

CREATE TABLE IF NOT EXISTS rmas (
    id             uuid PRIMARY KEY,
    version        bigint        NOT NULL DEFAULT 0,
    tenant_id      varchar(64)   NOT NULL,
    rma_number     varchar(40)   NOT NULL UNIQUE,
    order_id       uuid          NOT NULL REFERENCES orders (id),
    rma_type       varchar(16)   NOT NULL,
    status         varchar(24)   NOT NULL,
    reason         varchar(500),
    refund_total   numeric(18,2),
    child_order_id uuid REFERENCES orders (id),
    created_by     varchar(64),
    created_at     timestamptz   NOT NULL,
    updated_at     timestamptz   NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rmas_order ON rmas (order_id);

CREATE TABLE IF NOT EXISTS rma_items (
    id              uuid PRIMARY KEY,
    rma_id          uuid        NOT NULL REFERENCES rmas (id),
    order_item_id   uuid        NOT NULL REFERENCES order_items (id),
    sku             varchar(64) NOT NULL,
    quantity        integer     NOT NULL CHECK (quantity > 0),
    accepted_qty    integer     NOT NULL DEFAULT 0,
    rejected_qty    integer     NOT NULL DEFAULT 0,
    return_node_id  varchar(64),
    replacement_sku varchar(64),
    refund_amount   numeric(18,2)
);
CREATE INDEX IF NOT EXISTS idx_rma_items_rma ON rma_items (rma_id);
CREATE INDEX IF NOT EXISTS idx_rma_items_order_item ON rma_items (order_item_id);
