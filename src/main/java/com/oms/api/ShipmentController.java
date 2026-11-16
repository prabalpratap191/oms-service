package com.oms.api;

import com.oms.api.dto.PackShipmentRequest;
import com.oms.api.dto.Views;
import com.oms.domain.Order;
import com.oms.domain.Shipment;
import com.oms.domain.ShipmentItem;
import com.oms.domain.ShipmentStatus;
import com.oms.service.ShipmentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/oms/shipments")
@RequiredArgsConstructor
public class ShipmentController {
    private final ShipmentService shipmentService;

    @GetMapping
    public ResponseEntity<Views.ShipmentPage> listShipments(
        @RequestHeader(value = "X-Organization-Id", required = false) String organizationId,
        @RequestParam(required = false) String warehouseId,
        @RequestParam(required = false, defaultValue = "PLANNED,READY_TO_PICK,PICK_IN_PROGRESS,PICK_COMPLETED,PLANNED,PICK_IN_PROGRESS") String statuses,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {
        return ResponseEntity.ok(shipmentService.listActiveShipments(organizationId, warehouseId, statuses, page, size));
    }

    @PostMapping("/{shipmentId}/pack")
    public ResponseEntity<Views.ShipmentViewForPacking> packShipment(
        @PathVariable UUID shipmentId,
        @RequestHeader(value = "X-Organization-Id", required = true) String organizationId,
        @Valid @RequestBody PackShipmentRequest request) {
        return ResponseEntity.ok(shipmentService.packShipment(shipmentId, organizationId, request));
    }

    @PostMapping("/{shipmentId}/ready-to-ship")
    public ResponseEntity<Views.ShipmentViewForPacking> readyToShip(
        @PathVariable UUID shipmentId,
        @RequestHeader(value = "X-Organization-Id", required = true) String organizationId) {
        return ResponseEntity.ok(shipmentService.markReadyToShip(shipmentId, organizationId));
    }

    @PostMapping("/{shipmentId}/ship")
    public ResponseEntity<Views.ShipmentViewForPacking> ship(
        @PathVariable UUID shipmentId,
        @RequestHeader(value = "X-Organization-Id", required = true) String organizationId) {
        return ResponseEntity.ok(shipmentService.shipShipment(shipmentId, organizationId));
    }

    @PostMapping("/{shipmentId}/deliver")
    public ResponseEntity<Views.ShipmentViewForPacking> deliver(
        @PathVariable UUID shipmentId,
        @RequestHeader(value = "X-Organization-Id", required = true) String organizationId) {
        return ResponseEntity.ok(shipmentService.deliverShipment(shipmentId, organizationId));
    }
}
