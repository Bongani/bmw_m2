package com.bmw.m2.processor.state;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks a single watermark per partition to handle out-of-order events.
 *
 * The watermark advances monotonically and represents the furthest point in event-time
 * that the processor is confident it has seen all events up to (minus allowedLateness).
 *
 * Clicks contribute their full eventTime. Page views contribute eventTime - allowedLateness
 * so that a click arriving up to allowedLateness after its page view is never rejected as
 * late purely because the page view already advanced the watermark past it.
 */
@Slf4j
@Component
public class WatermarkTracker {

    private final Duration allowedLateness;
    private final Duration attributionWindow;
    private final MeterRegistry meterRegistry;
    // Hint: Use ConcurrentHashMap<Integer, Instant> for thread-safe partition watermarks
    private final ConcurrentHashMap<Integer, Instant> partitionWatermarkHashMap = new ConcurrentHashMap<>();
    // Partitions are dynamic (not known at startup), so gauges are registered lazily on the first updateWatermark call per partition
    private final Set<Integer> registeredPartitions = ConcurrentHashMap.newKeySet();

    public WatermarkTracker(
            @Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes,
            @Value("${watermark.attribution-window-minutes:30}") int attributionWindowMinutes,
            MeterRegistry meterRegistry) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        this.attributionWindow = Duration.ofMinutes(attributionWindowMinutes);
        this.meterRegistry = meterRegistry;
        log.info("Initialized WatermarkTracker with allowed lateness: {} minutes, attribution window: {} minutes",
                allowedLatenessMinutes, attributionWindowMinutes);
    }

    /**
     * Advance the watermark for a partition. Monotonic — never goes backward.
     *
     * @param partition the Kafka partition ID
     * @param eventTime the value to advance toward (caller is responsible for applying
     *                  the conservative shift for page views before calling)
     */
    public void updateWatermark(int partition, Instant eventTime) {
        Instant current = getWatermark(partition);
        if (eventTime.isAfter(current)) {
            partitionWatermarkHashMap.put(partition, eventTime);

            registerWatermarkGauge(partition);
            log.debug("Watermark advanced for partition {}: {} -> {}", partition, current, eventTime);
        } else {
            log.debug("Watermark unchanged for partition {} (event time {} is not after current {})", partition, eventTime, current);
        }
    }

    /**
     * Registers a Micrometer gauge for the watermark position of a partition, once per partition.
     * Micrometer holds a reference to partitionWatermarkHashMap and calls this lambda every time Prometheus scrapes (every 15s).
     * It reads the current value from the map at scrape time — so it always reflects the latest watermark
     *
     */
    private void registerWatermarkGauge(int partition) {
        if (registeredPartitions.add(partition)) {
            meterRegistry.gauge("watermark.position.seconds",
                    Tags.of("partition", String.valueOf(partition)),
                    partitionWatermarkHashMap,
                    map -> map.getOrDefault(partition, Instant.MIN).getEpochSecond());
        }
    }

    /**
     * Get current watermark for a partition.
     *
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(int partition) {
        return partitionWatermarkHashMap.getOrDefault(partition, Instant.MIN);
    }

    /**
     * Check if an event is too late (event_time < watermark - allowedLateness).
     *
     * @return true if the event is too late and should be dropped
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        Instant watermark = getWatermark(partition);
        if (watermark.equals(Instant.MIN)) {
            return false;
        }
        Instant cutoffTime = watermark.minus(allowedLateness);
        return eventTime.isBefore(cutoffTime);
    }

    /**
     * Get the minimum watermark across all partitions.
     *
     * @return minimum watermark, or Instant.MIN if no events have been processed
     */
    public Instant getMinWatermark() {
        if (partitionWatermarkHashMap.isEmpty()) {
            return Instant.MIN;
        }
        return Collections.min(partitionWatermarkHashMap.values());
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }

    public Duration getAttributionWindow() {
        return attributionWindow;
    }
}