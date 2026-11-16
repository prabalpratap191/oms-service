package com.oms.repository;

import com.oms.domain.Order;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {
    Optional<Order> findByIdempotencyKey(String key);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from Order o where o.id = :id")
    Optional<Order> findByIdForUpdate(@Param("id") UUID id);

    Page<Order> findByTenantIdAndCustomerIdOrderByCreatedAtDesc(String tenantId, String customerId, Pageable p);

    @Query(value = "select nextval('order_number_seq')", nativeQuery = true)
    Long nextOrderSeq();
}
