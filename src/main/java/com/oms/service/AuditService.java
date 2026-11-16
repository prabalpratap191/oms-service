package com.oms.service;

import com.oms.common.Actor;
import com.oms.domain.AuditLog;
import com.oms.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** Immutable audit trail: who (CUSTOMER / SYSTEM / CSR + id) moved which aggregate between which states. */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuditService {
    private final AuditLogRepository repo;

    public void record(String tenantId, String aggregateType, String aggregateId, String action,
                       String from, String to, Actor actor, String details) {
        AuditLog a = new AuditLog();
        a.setTenantId(tenantId); a.setAggregateType(aggregateType); a.setAggregateId(aggregateId);
        a.setAction(action); a.setFromStatus(from); a.setToStatus(to);
        a.setActorType(actor.type()); a.setActorId(actor.id());
        a.setDetails(details == null ? null : (details.length() > 2000 ? details.substring(0, 2000) : details));
        repo.save(a);
    }
}
