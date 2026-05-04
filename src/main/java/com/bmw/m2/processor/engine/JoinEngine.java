package com.bmw.m2.processor.engine;

import com.bmw.m2.processor.model.AdClickEvent;
import com.bmw.m2.processor.model.AttributedPageView;
import com.bmw.m2.processor.model.PageViewEvent;
import com.bmw.m2.processor.output.LateEventSink;
import com.bmw.m2.processor.output.OutputSink;
import com.bmw.m2.processor.state.ClickStateStore;
import com.bmw.m2.processor.state.WatermarkTracker;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 *
 * For each page_view, find the most recent ad_click for the same user within 30 minutes
 * before the page view in event time.
 *
 * Page views are held in a per-partition buffer until the watermark advances past
 * pageView.eventTime + allowedLateness. Clicks contribute their full eventTime to the
 * watermark; page views contribute eventTime - allowedLateness so that a click arriving
 * within the lateness window after its page view is never rejected as late.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;
    private final LateEventSink lateEventSink;
    private final MeterRegistry meterRegistry;

    // Per-partition buffer of page views waiting for the watermark to advance
    private final ConcurrentHashMap<Integer, List<PageViewEvent>> pageViewBufferHashMap = new ConcurrentHashMap<>();

    /**
     * Process an ad click event. Store the click in state for future attribution.
     *
     * Late clicks (event_time < watermark - allowedLateness) are written to the late
     * event sink and dropped. Valid clicks advance the watermark with their full eventTime,
     * which may unblock buffered page views waiting to be emitted.
     */
    public void processClick(AdClickEvent click) throws SQLException {
        log.debug("Processing click: {}", click.getClickId());
        int partition = click.getPartition();
        Instant eventTime = click.getEventTime();
        Instant currentWatermark = watermarkTracker.getWatermark(partition);

        if (watermarkTracker.isTooLate(partition, eventTime)) {
            log.warn("Late click dropped: {} at {} (watermark: {})", click.getClickId(), eventTime, currentWatermark);
            meterRegistry.counter("events.late.dropped", "type", "ad_click").increment();
            try {
                lateEventSink.write(click.getClickId(), "ad_click", eventTime,
                        partition, click.getOffset(), currentWatermark);
            } catch (SQLException e) {
                // swallow the exception to avoid retrying the click on the consumer as this does not break business logic
                log.error("Failed to write late click {} to late event sink", click.getClickId(), e);
            }
            return;
        }

        clickStore.addClick(click);
        meterRegistry.counter("events.clicks.processed").increment();
        watermarkTracker.updateWatermark(partition, eventTime);
        emitSafePageViews(partition, watermarkTracker.getWatermark(partition));
    }

    /**
     * Process a page view event.
     *
     * Late page views (event_time < watermark - allowedLateness) are written to the late
     * event sink and dropped. Valid page views are buffered and advance the watermark
     * conservatively by eventTime - allowedLateness. This ensures a click arriving up to
     * allowedLateness after the page view is never rejected as late by the page view's
     * own watermark contribution.
     */
    public void processPageView(PageViewEvent pageView) throws SQLException {
        log.debug("Processing page view: {}", pageView.getEventId());
        int partition = pageView.getPartition();
        Instant eventTime = pageView.getEventTime();
        Instant currentWatermark = watermarkTracker.getWatermark(partition);

        if (watermarkTracker.isTooLate(partition, eventTime)) {
            log.warn("Late page view dropped: {} at {} (watermark: {})", pageView.getEventId(), eventTime, currentWatermark);
            meterRegistry.counter("events.late.dropped", "type", "page_view").increment();
            try {
                lateEventSink.write(pageView.getEventId(), "page_view", eventTime,
                        partition, pageView.getOffset(), currentWatermark);
            } catch (SQLException e) {
                // swallow — late event logging is best-effort; don't retry the page view
                log.error("Failed to write late page view {} to late event sink", pageView.getEventId(), e);
            }
            return;
        }

        meterRegistry.counter("events.pageviews.processed").increment();

        // Buffer before updating the watermark so no concurrent flush can miss this page view
        if (!pageViewBufferHashMap.containsKey(partition)) {
            pageViewBufferHashMap.put(partition, new ArrayList<>());
        }
        pageViewBufferHashMap.get(partition).add(pageView);

        // Page views advance the watermark by eventTime - allowedLateness (conservative shift).
        // This keeps the watermark from overtaking the lateness window of any in-flight click.
        watermarkTracker.updateWatermark(partition, eventTime.minus(watermarkTracker.getAllowedLateness()));

        emitSafePageViews(partition, watermarkTracker.getWatermark(partition));
    }

    /**
     * Scheduled task that evicts old clicks and flushes any page views stranded in the
     * buffer when no further Kafka events arrive after the last buffered page view.
     * Runs every 30 seconds driven by wall clock, independent of Kafka traffic.
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldClicks() {
        Instant minWatermark = watermarkTracker.getMinWatermark();

        if (minWatermark != Instant.MIN) {
            Instant cutoffTime = minWatermark
                    .minus(watermarkTracker.getAttributionWindow())
                    .minus(watermarkTracker.getAllowedLateness());
            int evicted = clickStore.evictOldClicks(cutoffTime);
            meterRegistry.counter("state.clicks.evicted").increment(evicted);
            log.debug("Evicted {} clicks older than {} (minWatermark: {})", evicted, cutoffTime, minWatermark);
        } else {
            log.debug("Skipping click eviction — no watermark established yet");
        }

        // Always flush — wall-clock time is always past any real event's safe-emit threshold.
        // Covers the case where no further Kafka events arrive after the last buffered page view.
        Instant now = Instant.now();
        for (int partition : pageViewBufferHashMap.keySet()) {
            try {
                emitSafePageViews(partition, now);
            } catch (SQLException e) {
                log.error("Failed to flush idle page views for partition {}", partition, e);
            }
        }
    }

    /**
     * Check buffered page views for a partition and emit any that are now safe.
     * A page view is safe to emit when: watermark >= pageView.eventTime + allowedLateness
     *
     * @param watermark the watermark to use for the safe-emit check. Normal calls pass the
     *                  current partition watermark. The idle-flush scheduler passes
     *                  Instant.now() to drain the buffer when no Kafka events are arriving.
     */
    private void emitSafePageViews(int partition, Instant watermark) throws SQLException {
        List<PageViewEvent> partitionBuffer = pageViewBufferHashMap.get(partition);
        if (partitionBuffer == null || partitionBuffer.isEmpty()) {
            return;
        }

        List<PageViewEvent> toEmit = new ArrayList<>();
        for (PageViewEvent pageView : partitionBuffer) {
            Instant safeEmitTime = pageView.getEventTime().plus(watermarkTracker.getAllowedLateness());
            if (!watermark.isBefore(safeEmitTime)) {
                toEmit.add(pageView);
            }
        }

        for (PageViewEvent pageView : toEmit) {
            emitPageView(pageView);
            partitionBuffer.remove(pageView);
        }

        log.debug("Flushed {} page views for partition {}, {} remaining in buffer",
                toEmit.size(), partition, partitionBuffer.size());
    }

    /**
     * Attribute and write a single page view to the output sink.
     * SQLException propagates to StreamConsumer so the offset is not acknowledged on failure.
     */
    private void emitPageView(PageViewEvent pageView) throws SQLException {
        AdClickEvent attributableClick = clickStore.findAttributableClick(
                pageView.getUserId(), pageView.getEventTime());

        String campaignId = attributableClick != null ? attributableClick.getCampaignId() : null;
        String clickId = attributableClick != null ? attributableClick.getClickId() : null;

        AttributedPageView record = AttributedPageView.builder()
                .pageViewId(pageView.getEventId())
                .userId(pageView.getUserId())
                .eventTime(pageView.getEventTime())
                .url(pageView.getUrl())
                .attributedCampaignId(campaignId)
                .attributedClickId(clickId)
                .build();

        outputSink.write(record);
        if (campaignId != null) {
            meterRegistry.counter("attributions.matched").increment();
        } else {
            meterRegistry.counter("attributions.unmatched").increment();
        }
        log.debug("Emitted page view {} attributed to click {}", pageView.getEventId(), clickId);
    }
}