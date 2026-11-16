package com.oms.repository;

import com.oms.domain.*;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.*;

public interface RmaRepository extends JpaRepository<Rma, UUID> {
    List<Rma> findByOrderId(UUID orderId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select r from Rma r where r.id = :id")
    Optional<Rma> findByIdForUpdate(@Param("id") UUID id);

    @Query("select coalesce(sum(i.quantity), 0L) from Rma r join r.items i where i.orderItemId = :oid and r.status in :statuses")
    Long pendingQty(@Param("oid") UUID orderItemId, @Param("statuses") Collection<RmaStatus> statuses);

    @Query(value = "select nextval('rma_number_seq')", nativeQuery = true)
    Long nextRmaSeq();
}
