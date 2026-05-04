package com.bmw.m2.processor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for concurrent, partition-aware processing.
 *
 * Key design decisions:
 * - Manual acknowledgment mode for offset control
 * - Concurrent consumers (one thread per partition)
 * - Disable auto-commit for safety
 * - Enable idempotence through consumer configuration
 */
@Configuration
@EnableScheduling
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id:stream-processor-group}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    /**
     * Error handler shared by both listener container factories.
     *
     * Unrecoverable parse errors (malformed JSON, wrong types, unknown fields) are routed
     * directly to the dead-letter topic without retrying — retrying bad JSON would never succeed.
     * Recoverable errors (e.g. transient DB failures) are retried up to 3 times with no delay.
     * Dead-letter topics are auto-created by Kafka: ad_clicks.DLT and page_views.DLT.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 2L));

        // Bad JSON will never succeed on retry — skip straight to DLT
        handler.addNotRetryableExceptions(JsonProcessingException.class);

        return handler;
    }

    /**
     * Base consumer configuration shared by all consumers.
     */
    private Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        // Connection
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Deserialization
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Offset management - MANUAL control for at-least-once delivery
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Performance and reliability
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds

        // Fetch configuration
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // Isolation level for exactly-once semantics (if producers use transactions)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return props;
    }

    /**
     * Consumer factory for ad click events.
     */
    @Bean
    public ConsumerFactory<String, String> adClickConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * Listener container factory for ad click events.
     * Configured for concurrent processing with manual acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> adClickListenerContainerFactory(
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(adClickConsumerFactory());

        // Concurrency: one thread per partition (up to configured max)
        factory.setConcurrency(concurrency);

        // Route unrecoverable parse errors to DLT; retry transient failures up to 3 times
        factory.setCommonErrorHandler(errorHandler);

        // Manual acknowledgment for offset control
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Preserve partition ordering within each partition
        factory.getContainerProperties().setMissingTopicsFatal(false);

        return factory;
    }

    /**
     * Consumer factory for page view events.
     */
    @Bean
    public ConsumerFactory<String, String> pageViewConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    /**
     * Listener container factory for page view events.
     * Configured for concurrent processing with manual acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> pageViewListenerContainerFactory(
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(pageViewConsumerFactory());

        // Concurrency: one thread per partition (up to configured max)
        factory.setConcurrency(concurrency);

        // Route unrecoverable parse errors to DLT; retry transient failures up to 3 times
        factory.setCommonErrorHandler(errorHandler);

        // Manual acknowledgment for offset control
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Preserve partition ordering within each partition
        factory.getContainerProperties().setMissingTopicsFatal(false);

        return factory;
    }
}
