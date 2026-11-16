package com.oms.api;

import com.oms.api.dto.Requests.*;
import com.oms.api.dto.Views.*;
import com.oms.common.Actor;
import com.oms.service.CsrService;
import com.oms.service.RmaService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

/** CSR workbench. All calls require X-Actor-Type: CSR and X-Actor-Id: <csr user id>. */
@Slf4j
@RestController
@RequestMapping("/api/v1/csr")
@RequiredArgsConstructor
public class CsrController {
    private final CsrService csr;
    private final RmaService rmas;

    private static Actor actor(String type, String id) { return Actor.of(type, id).requireCsr(); }

    @PostMapping("/orders/{id}/hold")
    public OrderView hold(@PathVariable UUID id, @Valid @RequestBody HoldRequest r, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR hold requested for order {} reason={} actorType={} actorId={}", id, r.reason(), t, a);
        return csr.hold(id, r.reason(), actor(t, a));
    }

    @PostMapping("/orders/{id}/release-hold")
    public OrderView release(@PathVariable UUID id, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR release-hold requested for order {} actorType={} actorId={}", id, t, a);
        return csr.release(id, actor(t, a));
    }

    @PostMapping("/orders/{id}/priority")
    public OrderView priority(@PathVariable UUID id, @Valid @RequestBody PriorityRequest r, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR priority update requested for order {} priority={} actorType={} actorId={}", id, r.priority(), t, a);
        return csr.priority(id, r.priority(), actor(t, a));
    }

    @PostMapping("/orders/{id}/reconcile-payment")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void reconcile(@PathVariable UUID id, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR payment reconciliation requested for order {} actorType={} actorId={}", id, t, a);
        csr.reconcilePayment(id, actor(t, a));
    }

    @GetMapping("/rmas/{id}")
    public RmaView rma(@PathVariable UUID id) {
        log.info("CSR retrieving RMA {}", id);
        return rmas.get(id);
    }

    @PostMapping("/rmas/{id}/approve")
    public RmaView approve(@PathVariable UUID id, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR approving RMA {} actorType={} actorId={}", id, t, a);
        return rmas.approve(id, actor(t, a));
    }

    @PostMapping("/rmas/{id}/reject")
    public RmaView reject(@PathVariable UUID id, @Valid @RequestBody RejectRequest r, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR rejecting RMA {} reason={} actorType={} actorId={}", id, r.reason(), t, a);
        return rmas.reject(id, r.reason(), actor(t, a));
    }

    @PostMapping("/rmas/{id}/retry-refund")
    public RmaView retryRefund(@PathVariable UUID id, @RequestHeader("X-Actor-Type") String t, @RequestHeader("X-Actor-Id") String a) {
        log.info("CSR retrying refund for RMA {} actorType={} actorId={}", id, t, a);
        return rmas.retryRefund(id, actor(t, a));
    }
}
