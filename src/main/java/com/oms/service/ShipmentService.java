package com.oms.service;

import com.oms.api.dto.PackShipmentRequest;
import com.oms.api.dto.Views;
import com.oms.common.InvalidStateException;
import com.oms.common.NotFoundException;
import com.oms.domain.*;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.repository.OrderRepository;
import com.oms.repository.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShipmentService {
    private final ShipmentRepository shipments;
    private final OrderRepository orders;
    private final OutboxService outbox;
    private final SagaOrchestrator saga;

    @Transactional
    public Views.ShipmentViewForPacking markReadyToShip(UUID shipmentId, String organizationId) {
        Shipment shipment = shipments.findById(shipmentId)
            .orElseThrow(() -> new NotFoundException("Shipment not found: " + shipmentId));

        if (organizationId != null && !organizationId.isBlank() && !organizationId.equals(shipment.getTenantId())) {
            throw new IllegalArgumentException("Organization mismatch for shipment " + shipmentId);
        }

        if (shipment.getStatus() == ShipmentStatus.CANCELLED) {
            throw new InvalidStateException("Shipment is cancelled and cannot be moved to READY_TO_SHIP");
        }
        if (shipment.getStatus() != ShipmentStatus.PICK_COMPLETED && shipment.getStatus() != ShipmentStatus.PACKED && shipment.getStatus() != ShipmentStatus.READY_TO_SHIP) {
            throw new InvalidStateException("Shipment must be PACKED before it can be marked READY_TO_SHIP");
        }

        shipment.setStatus(ShipmentStatus.READY_TO_SHIP);
        shipment.getItems().forEach(item -> {
            if (item.getPackedQty() >= item.getQuantity()) item.setStatus(ShipmentLineStatus.PACKED);
            else if (item.getPackedQty() > 0) item.setStatus(ShipmentLineStatus.PICK_COMPLETED);
            else item.setStatus(ShipmentLineStatus.READY_TO_PICK);
        });

        Order order = orders.findById(shipment.getOrderId()).orElseThrow(() -> new NotFoundException("Order not found: " + shipment.getOrderId()));
        boolean allShipmentsReady = shipments.findByOrderId(order.getId()).stream()
            .filter(s -> s.getStatus() != ShipmentStatus.CANCELLED)
            .allMatch(s -> s.getStatus() == ShipmentStatus.READY_TO_SHIP || s.getStatus() == ShipmentStatus.DISPATCH_REQUESTED || s.getStatus() == ShipmentStatus.DISPATCHED || s.getStatus() == ShipmentStatus.DELIVERED);
        if (allShipmentsReady) {
            order.setStatus(OrderStatus.READY_TO_SHIP);
            order.getItems().forEach(item -> {
                if (item.getPackedQty() >= item.getAllocatedQty()) item.setStatus(OrderLineStatus.PACKED);
                else if (item.getPackedQty() > 0) item.setStatus(OrderLineStatus.PICK_COMPLETED);
            });
            outbox.enqueue(KafkaTopics.OMS_NOTIFICATION_ORDER_LIFECYCLE, order.getId().toString(), "OrderReadyToShipEvent",
                "Order", order.getId().toString(), order.getTenantId(),
                new Events.OrderReadyToShipEvent(order.getId(), order.getOrderNumber(), order.getTenantId(), Instant.now()));
        }

        return toPackingView(shipment);
    }

    @Transactional
    public Views.ShipmentViewForPacking shipShipment(UUID shipmentId, String organizationId) {
        Shipment shipment = shipments.findById(shipmentId)
            .orElseThrow(() -> new NotFoundException("Shipment not found: " + shipmentId));

        if (organizationId != null && !organizationId.isBlank() && !organizationId.equals(shipment.getTenantId())) {
            throw new IllegalArgumentException("Organization mismatch for shipment " + shipmentId);
        }

        if(shipment.getStatus() != ShipmentStatus.READY_TO_SHIP && shipment.getStatus() != ShipmentStatus.DISPATCH_REQUESTED) {
            throw new InvalidStateException("Shipment must be READY_TO_SHIP or DISPATCH_REQUESTED before it can be marked DISPATCHED");
        }
       
        String courier = "INSTANT".equals(shipment.getDeliveryClass()) ? "HYPERLOCAL-FLEET" : "TWO_DAY".equals(shipment.getDeliveryClass()) ? "BLUEDART" : "DELHIVERY";
        String tracking = "TRK" + Long.toString(Math.abs(shipment.getId().getMostSignificantBits()), 36).toUpperCase();
        shipment.setCourier(courier);
        shipment.setTrackingNumber(tracking);

        Order order = orders.findById(shipment.getOrderId()).orElseThrow(() -> new NotFoundException("Order not found: " + shipment.getOrderId()));
        saga.onShipmentStatus(new Events.ShipmentStatusUpdate(order.getId(), shipment.getId(), ShipmentStatus.DISPATCHED.name(), Instant.now()));
        return toPackingView(shipments.findById(shipmentId).orElseThrow(() -> new NotFoundException("Shipment not found: " + shipmentId)));
    }

    

    @Transactional
    public Views.ShipmentViewForPacking deliverShipment(UUID shipmentId, String organizationId) {
        Shipment shipment = shipments.findById(shipmentId)
            .orElseThrow(() -> new NotFoundException("Shipment not found: " + shipmentId));

        if (organizationId != null && !organizationId.isBlank() && !organizationId.equals(shipment.getTenantId())) {
            throw new IllegalArgumentException("Organization mismatch for shipment " + shipmentId);
        }

        Order order = orders.findById(shipment.getOrderId()).orElseThrow(() -> new NotFoundException("Order not found: " + shipment.getOrderId()));
        saga.onShipmentStatus(new Events.ShipmentStatusUpdate(order.getId(), shipment.getId(), ShipmentStatus.DELIVERED.name(), Instant.now()));
        return toPackingView(shipments.findById(shipmentId).orElseThrow(() -> new NotFoundException("Shipment not found: " + shipmentId)));
    }

    @Transactional(readOnly = true)
    public Views.ShipmentPage listActiveShipments(String organizationId, String warehouseId, String statuses, int page, int size) {
        List<ShipmentStatus> allowed = new ArrayList<>();
        if (statuses == null || statuses.isBlank()) {
            allowed.addAll(List.of(ShipmentStatus.READY_TO_PICK, ShipmentStatus.PICK_IN_PROGRESS, ShipmentStatus.PICK_COMPLETED));
        } else {
            for (String s : statuses.split(",")) {
                try {
                    allowed.add(ShipmentStatus.valueOf(s.trim().toUpperCase()));
                } catch (IllegalArgumentException ignored) {
                    // ignore invalid values
                }
            }
        }

        List<Shipment> all = shipments.findAll().stream()
            .filter(s -> organizationId == null || organizationId.isBlank() || organizationId.equals(s.getTenantId()))
            .filter(s -> warehouseId == null || warehouseId.isBlank() || warehouseId.equals(s.getWarehouseId()))
            .filter(s -> s.getStatus() != null && allowed.contains(s.getStatus()))
            .toList();

        int safePage = Math.max(page, 0);
        int safeSize = size <= 0 ? 20 : Math.min(size, 100);
        int from = Math.min(safePage * safeSize, all.size());
        int to = Math.min(from + safeSize, all.size());

        List<Views.ShipmentViewForPacking> rows = all.subList(from, to).stream()
            .map(this::toPackingView)
            .toList();

        return new Views.ShipmentPage(rows, all.size(), safePage, safeSize);
    }

    @Transactional
    public Views.ShipmentViewForPacking packShipment(UUID shipmentId, String organizationId, PackShipmentRequest request) {
        Shipment shipment = shipments.findById(shipmentId)
            .orElseThrow(() -> new NotFoundException("Shipment not found: " + shipmentId));

        if (organizationId != null && !organizationId.isBlank() && !organizationId.equals(shipment.getTenantId())) {
            throw new IllegalArgumentException("Organization mismatch for shipment " + shipmentId);
        }

        ShipmentItem line = shipment.getItems().stream()
            .filter(i -> i.getId().equals(request.shipmentLineId()))
            .findFirst()
            .orElseThrow(() -> new NotFoundException("Shipment line not found: " + request.shipmentLineId()));

        if (!line.getSku().equalsIgnoreCase(request.scannedBarcodeOrGtin()) && !request.scannedBarcodeOrGtin().equalsIgnoreCase(line.getSku())) {
            throw new IllegalArgumentException("Barcode does not match shipment line SKU");
        }

        if (request.quantityToPack() <= 0) {
            throw new IllegalArgumentException("Packing quantity must be greater than zero");
        }

        if (line.getPackedQty() + request.quantityToPack() > line.getQuantity()) {
            throw new IllegalArgumentException("Packing quantity exceeds allocated quantity");
        }

        line.pack(request.quantityToPack());
        if (line.isFullyPacked()) {
            line.setStatus(ShipmentLineStatus.PACKED);
        } else {
            line.setStatus(line.getPackedQty() > 0 ? ShipmentLineStatus.PICK_COMPLETED : ShipmentLineStatus.PICK_IN_PROGRESS);
        }

        Order order = orders.findById(shipment.getOrderId()).orElseThrow(() -> new NotFoundException("Order not found: " + shipment.getOrderId()));
        OrderItem orderItem = order.getItems().stream()
            .filter(i -> i.getSku().equalsIgnoreCase(line.getSku()) && java.util.Objects.equals(i.getWarehouseId(), line.getWarehouseId()))
            .findFirst()
            .orElse(null);

        if (orderItem != null) {
            int newPackedQty = Math.min(orderItem.getAllocatedQty(), orderItem.getPackedQty() + request.quantityToPack());
            orderItem.setPackedQty(newPackedQty);
            orderItem.setStatus(newPackedQty >= orderItem.getAllocatedQty() ? OrderLineStatus.PACKED : OrderLineStatus.PICK_COMPLETED);
        }

        shipment.setStatus(ShipmentStatus.PICK_IN_PROGRESS);
        if (shipment.getItems().stream().allMatch(ShipmentItem::isFullyPacked)) {
            shipment.setStatus(ShipmentStatus.PACKED);
            shipment.getItems().forEach(si -> si.setStatus(ShipmentLineStatus.PACKED));
        } else if (shipment.getItems().stream().anyMatch(si -> si.getPackedQty() > 0)) {
            shipment.setStatus(ShipmentStatus.PICK_COMPLETED);
        }

        if (order.getStatus() == null || order.getStatus() == OrderStatus.READY_TO_PICK || order.getStatus() == OrderStatus.SPLIT_PROCESSING) {
            order.setStatus(OrderStatus.PICK_IN_PROGRESS);
        }

        if (order.getItems().stream().allMatch(i -> i.getPackedQty() >= i.getAllocatedQty())) {
            order.setStatus(OrderStatus.PACKED);
        } else if (order.getItems().stream().anyMatch(i -> i.getPackedQty() > 0)) {
            order.setStatus(OrderStatus.PICK_IN_PROGRESS);
        }

        boolean allShipmentsPacked = shipments.findByOrderId(order.getId()).stream()
            .allMatch(s -> s.getStatus() == ShipmentStatus.PACKED);
        if (allShipmentsPacked && order.getStatus() != OrderStatus.CANCELLED) {
            order.setStatus(OrderStatus.READY_TO_SHIP);
        }

        Events.ShipmentItemPackedEvent event = new Events.ShipmentItemPackedEvent(
            order.getId(), shipment.getId(), line.getId(), orderItem != null ? orderItem.getId() : null, line.getSku(), shipment.getWarehouseId(),
            request.quantityToPack(), line.getPackedQty(), line.getQuantity(), Instant.now());
        outbox.enqueue(KafkaTopics.LOGISTICS_OMS_SHIPMENT_STATUS, shipment.getId().toString(), "ShipmentItemPackedEvent",
            "Shipment", shipment.getId().toString(), shipment.getTenantId(), event);

        if (shipment.getStatus() == ShipmentStatus.PACKED) {
            outbox.enqueue(KafkaTopics.LOGISTICS_OMS_SHIPMENT_STATUS, shipment.getId().toString(), "ShipmentPackedEvent",
                "Shipment", shipment.getId().toString(), shipment.getTenantId(),
                new Events.ShipmentPackedEvent(order.getId(), shipment.getId(), shipment.getWarehouseId(), Instant.now()));
        }

        if (allShipmentsPacked && order.getStatus() == OrderStatus.READY_TO_SHIP) {
            outbox.enqueue(KafkaTopics.OMS_NOTIFICATION_ORDER_LIFECYCLE, order.getId().toString(), "OrderReadyToShipEvent",
                "Order", order.getId().toString(), order.getTenantId(),
                new Events.OrderReadyToShipEvent(order.getId(), order.getOrderNumber(), order.getTenantId(), Instant.now()));
        }

        return toPackingView(shipment);
    }

    private Views.ShipmentViewForPacking toPackingView(Shipment shipment) {
        return new Views.ShipmentViewForPacking(
            shipment.getId(),
            shipment.getWarehouseId(),
            shipment.getStatus(),
            shipment.getFulfillmentShopId(),
            shipment.getShippingAddress(), shipment.getSubtotal().toPlainString(), shipment.getDeliveryFee().toPlainString(), shipment.getTaxAmount().toPlainString(), shipment.getTotalAmount().toPlainString(),
            shipment.getSlaWindowEnd(), shipment.getDispatchedAt(), shipment.getDeliveredAt(),
            shipment.getItems().stream().map(si -> new Views.ShipmentLineView(
                si.getId(), si.getSku(), si.getSku(), si.getQuantity(), si.getPackedQty(), si.getQuantity(), si.getStatus().name(), si.getWarehouseId())).toList());
    }
}
