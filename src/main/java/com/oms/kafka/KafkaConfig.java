package com.oms.kafka;

import com.oms.config.OmsProperties;
import com.oms.service.FailedEventService;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /** Declares every OMS topic; KafkaAdmin creates the missing ones on startup. */
    @Bean
    public KafkaAdmin.NewTopics omsTopics(OmsProperties props) {
        NewTopic[] topics = KafkaTopics.ALL.stream()
            .map(t -> TopicBuilder.name(t).partitions(props.kafka().partitions()).replicas(props.kafka().replicas()).build())
            .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topics);
    }

    /**
     * Failed record -> retried exactly ONE more time (1s back-off) -> then persisted to failed_events
     * (offset is committed so the partition is never blocked). Re-drive via the admin API.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(FailedEventService failedEvents) {
        return new DefaultErrorHandler((record, ex) -> failedEvents.recordConsumerFailure(record, ex), new FixedBackOff(1000L, 1L));
    }
}
