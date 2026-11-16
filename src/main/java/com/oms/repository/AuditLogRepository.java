package com.oms.repository;

import com.oms.domain.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;

public interface AuditLogRepository extends JpaRepository<AuditLog, UUID> {
    List<AuditLog> findByTenantIdAndAggregateIdInOrderByCreatedAtAsc(String tenantId, Collection<String> ids);
}
