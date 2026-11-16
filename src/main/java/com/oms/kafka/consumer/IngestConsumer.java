package com.oms.kafka.consumer;

import com.oms.api.dto.CreateOrderRequest;
import com.oms.common.Actor;
import com.oms.domain.ActorType;
import com.oms.kafka.KafkaGroups;
import com.oms.kafka.KafkaTopics;
import com.oms.kafka.events.EventCodec;
import com.oms.kafka.events.EventEnvelope;
import com.oms.kafka.events.Events;
import com.oms.service.InboxService;
import com.oms.service.OrderService;
import com.oms.service.OutboxService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.Set;
import java.util.stream.Collectors;

/** External marketplace ingestion (ONDC / Amazon ...). Replies on oms.marketplace.orderIngestResult (via outbox). Not class-transactional: createOrder manages its own tx. */
@Slf4j
@Component
@RequiredArgsConstructor
public class IngestConsumer {
    private final EventCodec codec;
    private final Validator validator;
    private final OrderService orderService;
    private final OutboxService outbox;
    private final TransactionTemplate tx;
    private final InboxService inbox;

    @KafkaListener(topics = KafkaTopics.MARKETPLACE_OMS_ORDER_INGEST, groupId = KafkaGroups.OMS_INGEST)
    public void onOrderIngest(ConsumerRecord<String, String> rec) {
        log.info("Kafka consumer received topic={} partition={} offset={} key={} eventId={}",
            rec.topic(), rec.partition(), rec.offset(), rec.key(), rec.headers().lastHeader("eventId") != null ? new String(rec.headers().lastHeader("eventId").value()) : "unknown");
        EventEnvelope<CreateOrderRequest> env = codec.parse(rec.value(), CreateOrderRequest.class);
        CreateOrderRequest req = env.payload();
        Set<ConstraintViolation<CreateOrderRequest>> violations = validator.validate(req);
        if (!violations.isEmpty()) {                                     // business rejection => failure response, not a retry
            String msg = violations.stream().map(v -> v.getPropertyPath() + " " + v.getMessage()).collect(Collectors.joining("; "));
            log.warn("Order ingest rejected for externalOrderRef={} reason={}", req.externalOrderRef(), msg);
            reply(env, new Events.OrderIngestResult(req.externalOrderRef(), null, null, false, msg));
            return;
        }
        try {
            OrderService.CreateResult res = orderService.createOrder(req, new Actor(ActorType.SYSTEM, "marketplace:" + req.channel()));
            log.info("Order ingest processed successfully externalOrderRef={} orderId={} orderNumber={}", req.externalOrderRef(), res.order().id(), res.order().orderNumber());
            reply(env, new Events.OrderIngestResult(req.externalOrderRef(), res.order().id(), res.order().orderNumber(), true, null));
        } catch (IllegalArgumentException bad) {
            log.warn("Order ingest failed externalOrderRef={} reason={}", req.externalOrderRef(), bad.getMessage());
            reply(env, new Events.OrderIngestResult(req.externalOrderRef(), null, null, false, bad.getMessage()));
        }
    }

    private void reply(EventEnvelope<CreateOrderRequest> env, Events.OrderIngestResult result) {
        tx.executeWithoutResult(st -> outbox.enqueue(KafkaTopics.OMS_MARKETPLACE_ORDER_INGEST_RESULT,
            result.externalOrderRef() == null ? env.eventId().toString() : result.externalOrderRef(), "OrderIngestResult", "Order",
            result.orderId() == null ? env.eventId().toString() : result.orderId().toString(), env.tenantId(), result));
    }
}
