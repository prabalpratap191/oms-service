package com.oms.kafka.consumer;

import com.oms.kafka.events.EventCodec;
import com.oms.kafka.events.EventEnvelope;
import com.oms.service.InboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import java.util.function.Consumer;

/** Parse -> de-duplicate -> handle. Must run inside the listener's transaction so a failure rolls back inbox + business writes. */
@Slf4j
@Component
@RequiredArgsConstructor
public class InboundSupport {
    private final EventCodec codec;
    private final InboxService inbox;

    public <T> void handle(ConsumerRecord<String, String> rec, Class<T> type, Consumer<EventEnvelope<T>> fn) {
        log.info("Kafka consumer received topic={} partition={} offset={} key={}",
            rec.topic(), rec.partition(), rec.offset(), rec.key());
        EventEnvelope<T> env = codec.parse(rec.value(), type);
        if (!inbox.firstTime(rec.topic(), env.eventId())) {
            log.info("Duplicate event {} on {} skipped", env.eventId(), rec.topic());
            return;
        }
        log.info("Handling Kafka event topic={} eventType={} eventId={} aggregateType={} aggregateId={}",
            rec.topic(), env.eventType(), env.eventId(), env.aggregateType(), env.aggregateId());
        fn.accept(env);
    }
}
