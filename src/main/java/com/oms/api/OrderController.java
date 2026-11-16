package com.oms.api;

import com.oms.api.dto.CreateOrderRequest;
import com.oms.api.dto.Views;
import com.oms.api.dto.Requests.*;
import com.oms.api.dto.Views.*;
import com.oms.common.Actor;
import com.oms.service.OrderService;
import com.oms.service.RmaService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

/** Actor is taken from X-Actor-Type (CUSTOMER|CSR|SYSTEM) / X-Actor-Id; authentication is expected at the gateway. */
@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orders;
    private final RmaService rmas;

    @PostMapping
    public ResponseEntity<OrderView> create(@Valid @RequestBody CreateOrderRequest req,
                                            @RequestHeader(value = "X-Actor-Type", required = false) String type,
                                            @RequestHeader(value = "X-Actor-Id", required = false) String id) {
        log.info("Creating order for tenantId={} customerId={} actorType={} actorId={}", req.tenantId(), req.customerId(), type, id);
        OrderService.CreateResult r = orders.createOrder(req, Actor.of(type, id == null ? req.customerId() : id));
        return ResponseEntity.status(r.created() ? HttpStatus.CREATED : HttpStatus.OK).body(r.order());
    }

    @GetMapping("/{id}")
    public Views.OrderDetailView getDetailed(@PathVariable UUID id) {
        log.info("Fetching detailed order {}", id);
        return orders.getDetailed(id);
    }

    @GetMapping
    public PageResult<OrderView> list(@RequestParam String tenantId, @RequestParam String customerId,
                                      @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        log.info("Listing orders tenantId={} customerId={} page={} size={}", tenantId, customerId, page, size);
        return orders.list(tenantId, customerId, page, size);
    }

    @GetMapping("/{id}/timeline")
    public TimelineView timeline(@PathVariable UUID id) {
        log.info("Fetching order timeline for order {}", id);
        return orders.timeline(id);
    }

    @PatchMapping("/{id}/delivery")
    public OrderView updateDelivery(@PathVariable UUID id, @Valid @RequestBody UpdateDeliveryRequest req,
                                    @RequestHeader(value = "X-Actor-Type", required = false) String type,
                                    @RequestHeader(value = "X-Actor-Id", required = false) String actor) {
        log.info("Updating delivery for order {} actorType={} actorId={}", id, type, actor);
        return orders.updateDelivery(id, req, Actor.of(type, actor));
    }

    @PatchMapping("/{id}/items")
    public OrderView amendItems(@PathVariable UUID id, @Valid @RequestBody AmendItemsRequest req,
                                @RequestHeader(value = "X-Actor-Type", required = false) String type,
                                @RequestHeader(value = "X-Actor-Id", required = false) String actor) {
        log.info("Amending items for order {} actorType={} actorId={}", id, type, actor);
        return orders.amendItems(id, req, Actor.of(type, actor));
    }

    @PostMapping("/{id}/cancel")
    public OrderView cancel(@PathVariable UUID id, @RequestBody(required = false) CancelRequest req,
                            @RequestHeader(value = "X-Actor-Type", required = false) String type,
                            @RequestHeader(value = "X-Actor-Id", required = false) String actor) {
        log.info("Cancelling order {} actorType={} actorId={}", id, type, actor);
        return orders.cancel(id, req, Actor.of(type, actor));
    }

    @PostMapping("/{id}/rmas")
    @ResponseStatus(HttpStatus.CREATED)
    public RmaView createRma(@PathVariable UUID id, @Valid @RequestBody RmaCreateRequest req,
                             @RequestHeader(value = "X-Actor-Type", required = false) String type,
                             @RequestHeader(value = "X-Actor-Id", required = false) String actor) {
        log.info("Creating RMA for order {} actorType={} actorId={}", id, type, actor);
        return rmas.create(id, req, Actor.of(type, actor));
    }

    @GetMapping("/{id}/rmas")
    public List<RmaView> listRmas(@PathVariable UUID id) {
        log.info("Listing RMAs for order {}", id);
        return rmas.forOrder(id);
    }

    @GetMapping("/orderSourcing")
    public Views.OrderSourcingPage orderSourcing(
            @RequestHeader(value = "X-Tenant-Id", required = true) String tenantId,
            @RequestHeader(value = "X-Organization-Id", required = true) String organizationId,
            @RequestParam(required = false) String deliveryStartDate,
            @RequestParam(required = false) String deliveryEndDate,
            @RequestParam(required = false) String orderStatus,
            @RequestParam(required = false) String paymentMethod,
            @RequestParam(required = false) String orderType,
            @RequestParam(required = false) String node,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        log.info("Fetching order sourcing rows tenantId={} organizationId={} deliveryStartDate={} deliveryEndDate={} orderStatus={} paymentMethod={} orderType={} node={} page={} size={}",
                tenantId, organizationId, deliveryStartDate, deliveryEndDate, orderStatus, paymentMethod, orderType, node, page, size);
        return orders.orderSourcing(tenantId, organizationId, deliveryStartDate, deliveryEndDate,
                orderStatus, paymentMethod, orderType, node, page, size);
    }
}
