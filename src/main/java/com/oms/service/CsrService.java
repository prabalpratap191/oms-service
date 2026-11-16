package com.oms.service;

import com.oms.api.dto.Views.OrderView;
import com.oms.common.*;
import com.oms.domain.*;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.Events;
import com.oms.repository.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

/** CSR workbench: hold (fraud), release, priority override, manual payment reconciliation. */
@Slf4j
@Service
@RequiredArgsConstructor
public class CsrService {
    private final SagaOrchestrator saga;
    private final AuditService audit;
    private final OutboxService outbox;
    private final ShipmentRepository shipments;

    @Transactional
    public OrderView hold(UUID id, String reason, Actor actor) {
        Order o = open(id, actor);
        o.setOnHold(true); o.setHoldReason(reason);
        audit.record(o.getTenantId(), "ORDER", id.toString(), "HOLD_PLACED", o.getStatus().name(), o.getStatus().name(), actor, reason);
        return view(o);
    }

    @Transactional
    public OrderView release(UUID id, Actor actor) {
        Order o = open(id, actor);
        if (!o.isOnHold()) throw new InvalidStateException("Order is not on hold");
        o.setOnHold(false); o.setHoldReason(null);
        audit.record(o.getTenantId(), "ORDER", id.toString(), "HOLD_RELEASED", o.getStatus().name(), o.getStatus().name(), actor, null);
        saga.emitCurrent(o, saga.sagaOf(id));          // re-emit the command of the paused step
        return view(o);
    }

    @Transactional
    public OrderView priority(UUID id, Priority p, Actor actor) {
        Order o = open(id, actor);
        Priority old = o.getPriority();
        o.setPriority(p);
        audit.record(o.getTenantId(), "ORDER", id.toString(), "PRIORITY_CHANGED", old.name(), p.name(), actor, null);
        return view(o);
    }

    @Transactional
    public void reconcilePayment(UUID id, Actor actor) {
        Order o = open(id, actor);
        audit.record(o.getTenantId(), "ORDER", id.toString(), "PAYMENT_RECONCILE_TRIGGERED", null, null, actor, null);
        outbox.enqueue(KafkaTopics.OMS_PAYMENT_PAYMENT_RECONCILE, id.toString(), "PaymentReconcileCommand", "Order", id.toString(), o.getTenantId(),
            new Events.PaymentReconcileCommand(id, o.getTenantId(), o.getCurrency(), o.payable()));
    }

    private Order open(UUID id, Actor actor) {
        actor.requireCsr();
        Order o = saga.lock(id);
        if (o.getStatus() == OrderStatus.DELIVERED || o.getStatus() == OrderStatus.CANCELLED) throw new InvalidStateException("Order is " + o.getStatus());
        return o;
    }

    private OrderView view(Order o) { return OrderView.from(o, shipments.findByOrderId(o.getId())); }
}
