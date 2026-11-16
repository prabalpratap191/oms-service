package com.oms.api;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

/** Live SLA reporting per region hub: order-to-fulfill latency, click-to-deliver turnaround, split-order ratio, SLA breaches. */
@Slf4j
@RestController
@RequestMapping("/api/v1/metrics")
@RequiredArgsConstructor
public class MetricsController {
    private final JdbcTemplate jdbc;

    @GetMapping("/sla")
    public List<Map<String, Object>> sla(@RequestParam String tenantId) {
        log.info("Retrieving SLA metrics for tenantId={}", tenantId);
        return jdbc.queryForList("""
            select region_code,
                   count(*)                                   as orders,
                   sum(shipment_count)                        as shipments,
                   round(sum(shipment_count)::numeric / nullif(count(*), 0), 2) as split_ratio,
                   round(avg(extract(epoch from (dispatched_at - created_at)))::numeric, 1) as avg_order_to_fulfill_sec,
                   round(avg(extract(epoch from (fulfilled_at  - created_at)))::numeric, 1) as avg_click_to_deliver_sec,
                   sum(breached)                              as sla_breaches
            from (select o.region_code, o.created_at, o.dispatched_at, o.fulfilled_at,
                         (select count(*) from shipments s where s.order_id = o.id and s.status <> 'CANCELLED') as shipment_count,
                         (select count(*) from shipments s where s.order_id = o.id and s.delivered_at > s.sla_window_end) as breached
                  from orders o where o.tenant_id = ? and o.order_type = 'STANDARD') x
            group by region_code order by region_code
            """, tenantId);
    }
}
