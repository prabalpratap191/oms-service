package com.oms.service;

import com.oms.domain.Order;
import com.oms.domain.OrderStatus;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;

/** Prometheus metrics: order-to-fulfill latency, click-to-deliver turnaround, split-order ratio per region. */
@Slf4j
@Component
@RequiredArgsConstructor
public class OmsMetrics {
    private final MeterRegistry registry;

    private static String region(Order o) { return o.getRegionCode() == null ? "NA" : o.getRegionCode(); }

    public void orderCreated(Order o) { registry.counter("oms.orders.created", "channel", o.getChannel().name()).increment(); }
    public void statusChanged(OrderStatus to) { registry.counter("oms.orders.status", "status", to.name()).increment(); }
    public void orderToFulfill(Order o, Instant at) {
        registry.timer("oms.order.to.fulfill", "region", region(o)).record(Duration.between(o.getCreatedAt(), at));
    }
    public void clickToDeliver(Order o, Instant at) {
        registry.timer("oms.click.to.deliver", "region", region(o)).record(Duration.between(o.getCreatedAt(), at));
    }
    public void splitRatio(Order o, int shipments) {
        DistributionSummary.builder("oms.order.shipments").tag("region", region(o)).register(registry).record(shipments);
    }
}
