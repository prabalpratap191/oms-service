package com.oms.repository;

import com.oms.domain.*;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;

public interface ShipmentRepository extends JpaRepository<Shipment, UUID> {
    List<Shipment> findByOrderId(UUID orderId);
    Optional<Shipment> findByIdAndTenantId(UUID shipmentId, String tenantId);
    Optional<Shipment> findByOrderIdAndFulfillmentShopIdAndDeliveryClass(UUID orderId, String shop, DeliveryClass dc);
}
