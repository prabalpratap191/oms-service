package com.oms.api;

import com.oms.domain.FailedEvent;
import com.oms.domain.FailedEventStatus;
import com.oms.service.FailedEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Operational API for events that failed twice (consumer or outbox) and were parked in failed_events. */
@Slf4j
@RestController
@RequestMapping("/api/v1/admin/failed-events")
@RequiredArgsConstructor
public class AdminController {
    private final FailedEventService failed;

    @GetMapping
    public List<FailedEvent> list(@RequestParam(required = false) FailedEventStatus status) {
        log.info("Listing failed events with status={}", status);
        return failed.list(status);
    }

    @PostMapping("/{id}/retry")
    public FailedEvent retry(@PathVariable UUID id) {
        log.info("Retrying failed event {}", id);
        return failed.retry(id);
    }

    @PostMapping("/retry-all")
    public Map<String, Integer> retryAll() {
        log.info("Retrying all failed events");
        return Map.of("retried", failed.retryAll());
    }
}
