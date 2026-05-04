package com.bmw.m2.processor.engine;

import com.bmw.m2.processor.model.AdClickEvent;
import com.bmw.m2.processor.model.AttributedPageView;
import com.bmw.m2.processor.model.PageViewEvent;
import com.bmw.m2.processor.output.LateEventSink;
import com.bmw.m2.processor.output.OutputSink;
import com.bmw.m2.processor.state.ClickStateStore;
import com.bmw.m2.processor.state.WatermarkTracker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class JoinEngineTest {

    // Base time: 2026-01-01T12:00:00Z (represents 12:00 in SCENARIOS.md)
    private static final Instant BASE = Instant.parse("2026-01-01T12:00:00Z");

    private JoinEngine joinEngine;
    private OutputSink outputSink;
    private LateEventSink lateEventSink;
    private Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        //in memory db
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");

        outputSink = new OutputSink();
        outputSink.initWithConnection(connection);

        lateEventSink = new LateEventSink();
        lateEventSink.initWithConnection(connection);

        WatermarkTracker watermarkTracker = new WatermarkTracker(2, 30);
        ClickStateStore clickStateStore = new ClickStateStore();

        joinEngine = new JoinEngine(clickStateStore, watermarkTracker, outputSink, lateEventSink, new SimpleMeterRegistry());
    }

    // --- Helpers ---

    private AdClickEvent click(String userId, String clickId, String campaignId, int minutesFromBase, int partition) {
        return AdClickEvent.builder()
                .userId(userId)
                .clickId(clickId)
                .campaignId(campaignId)
                .eventTime(BASE.plusSeconds(minutesFromBase * 60L))
                .partition(partition)
                .offset(0L)
                .build();
    }

    private PageViewEvent pageView(String userId, String eventId, String url, int minutesFromBase, int partition) {
        return PageViewEvent.builder()
                .userId(userId)
                .eventId(eventId)
                .url(url)
                .eventTime(BASE.plusSeconds(minutesFromBase * 60L))
                .partition(partition)
                .offset(0L)
                .build();
    }

    private AttributedPageView queryOutput(String pageViewId) throws Exception {
        String sql = "SELECT * FROM attributed_page_views WHERE page_view_id = ?";
        try (var stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, pageViewId);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) return null;
            AttributedPageView result = new AttributedPageView();
            result.setPageViewId(rs.getString("page_view_id"));
            result.setUserId(rs.getString("user_id"));
            result.setAttributedCampaignId(rs.getString("attributed_campaign_id"));
            result.setAttributedClickId(rs.getString("attributed_click_id"));
            return result;
        }
    }

    private boolean isInLateEvents(String eventId) throws Exception {
        String sql = "SELECT COUNT(*) FROM late_events WHERE event_id = ?";
        try (var stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, eventId);
            ResultSet rs = stmt.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        }
    }

    private int countRows(String pageViewId) throws Exception {
        String sql = "SELECT COUNT(*) FROM attributed_page_views WHERE page_view_id = ?";
        try (var stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, pageViewId);
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    // --- Scenario Tests ---

    @Test
    void normalClickBeforePageView() throws Exception {
        // user_1: click at +5min, page view at +10min — within 30min window
        joinEngine.processClick(click("user_1", "click_1", "campaign_A", 5, 0));
        joinEngine.processPageView(pageView("user_1", "pv_1", "/product1", 10, 0));

        // Advance watermark past safe-emit time: pv_1.eventTime(+10) + lateness(2) = +12min
        joinEngine.processClick(click("user_1", "click_advance", "campaign_X", 15, 0));

        AttributedPageView result = queryOutput("pv_1");
        assertNotNull(result);
        assertEquals("campaign_A", result.getAttributedCampaignId());
        assertEquals("click_1", result.getAttributedClickId());
    }

    @Test
    void outOfOrderClickArrivesAfterPageView() throws Exception {
        // user_2: page view at +15min arrives before click at +14min (event_time).
        // pv_2 contributes 15-2=13min to watermark → cutoff=11min.
        // click_2 at 12min > cutoff 11min → not late, stored correctly.
        // pv_2 stays buffered until watermark reaches 15+2=17min.
        joinEngine.processPageView(pageView("user_2", "pv_2", "/product2", 15, 0));
        joinEngine.processClick(click("user_2", "click_2", "campaign_B", 14, 0));

        // click_advance at +18min advances watermark to 18min >= 17min → pv_2 flushes
        joinEngine.processClick(click("user_2", "click_advance", "campaign_X", 18, 0));

        AttributedPageView result = queryOutput("pv_2");
        assertNotNull(result);
        assertEquals("campaign_B", result.getAttributedCampaignId());
        assertEquals("click_2", result.getAttributedClickId());
    }

    @Test
    void multipleClicksPicksMostRecent() throws Exception {
        // user_3: two clicks at +20min and +25min, page view at +30min — pick click_3b (latest)
        joinEngine.processClick(click("user_3", "click_3a", "campaign_C", 20, 0));
        joinEngine.processClick(click("user_3", "click_3b", "campaign_D", 25, 0));
        joinEngine.processPageView(pageView("user_3", "pv_3", "/product3", 30, 0));

        // Advance watermark past safe-emit time: pv_3.eventTime(+30) + lateness(2) = +32min
        joinEngine.processClick(click("user_3", "click_advance", "campaign_X", 35, 0));

        AttributedPageView result = queryOutput("pv_3");
        assertNotNull(result);
        assertEquals("campaign_D", result.getAttributedCampaignId());
        assertEquals("click_3b", result.getAttributedClickId());
    }

    @Test
    void clickOutsideAttributionWindow() throws Exception {
        // user_4: click at +35min, page view at +70min — 35min gap exceeds 30min window
        joinEngine.processClick(click("user_4", "click_4", "campaign_E", 35, 0));
        joinEngine.processPageView(pageView("user_4", "pv_4", "/product4", 70, 0));

        // Advance watermark past safe-emit time: pv_4.eventTime(+70) + lateness(2) = +72min
        joinEngine.processClick(click("user_4", "click_advance", "campaign_X", 73, 0));

        AttributedPageView result = queryOutput("pv_4");
        assertNotNull(result);
        assertNull(result.getAttributedCampaignId());
        assertNull(result.getAttributedClickId());
    }

    @Test
    void lateClickIsDropped() throws Exception {
        // user_5: page view at +45min contributes 45-2=43min to watermark → cutoff=41min.
        // click_5 at event_time +40min < cutoff 41min → dropped.
        joinEngine.processPageView(pageView("user_5", "pv_5", "/product5", 45, 0));
        joinEngine.processClick(click("user_5", "click_5", "campaign_F", 40, 0));

        assertTrue(isInLateEvents("click_5"));

        // Advance watermark past pv_5 safe-emit time (45+2=47min) to flush it with null attribution
        joinEngine.processClick(click("user_5", "click_advance", "campaign_X", 48, 0));

        AttributedPageView result = queryOutput("pv_5");
        assertNotNull(result);
        assertNull(result.getAttributedCampaignId());
    }

    @Test
    void pageViewWithNoClick() throws Exception {
        // user_6: page view at +80min, no click ever exists for this user.
        // No click arrives so the watermark only reaches 80-2=78min from the page view.
        // The scheduler idle flush uses Instant.now() to emit the buffered page view.
        joinEngine.processPageView(pageView("user_6", "pv_6", "/product6", 80, 0));
        joinEngine.evictOldClicks();

        AttributedPageView result = queryOutput("pv_6");
        assertNotNull(result);
        assertNull(result.getAttributedCampaignId());
        assertNull(result.getAttributedClickId());
    }

    @Test
    void idleFlushEmitsLastPageView() throws Exception {
        // pv_6 is buffered but the watermark never advances past safe-emit time
        // because no further Kafka event arrives. Verify it stays buffered until
        // evictOldClicks() fires, then is flushed via Instant.now().
        joinEngine.processPageView(pageView("user_6", "pv_6", "/product6", 80, 0));

        // Not yet emitted — watermark from pv_6 alone is only 78min (80-2), below safe-emit 82min
        assertNull(queryOutput("pv_6"));

        joinEngine.evictOldClicks();

        AttributedPageView result = queryOutput("pv_6");
        assertNotNull(result);
        assertNull(result.getAttributedCampaignId());
        assertNull(result.getAttributedClickId());
    }

    /**
     * The test needs to verify this specific sequence
     *  1. Events processed → outputSink.write() succeeds → row written to in memory SQLite
     *  2. Crash happens — acknowledgment.acknowledge() never called
     *  3. Kafka replays the same messages from the last committed offset
     *  4. outputSink.write() called again with the same pageViewId
     *  5. INSERT OR IGNORE silently discards the duplicate — still exactly 1 row
     *
     * @throws Exception
     */
    @Test
    void replayAfterCrashProducesNoDuplicates() throws Exception {
        // Round 1: process events — pv_1 gets written to SQLite
        joinEngine.processClick(click("user_1", "click_1", "campaign_A", 5, 0));
        joinEngine.processPageView(pageView("user_1", "pv_1", "/product1", 10, 0));
        // Advance watermark past safe-emit time so pv_1 is flushed to SQLite
        joinEngine.processClick(click("user_1", "click_advance", "campaign_X", 15, 0));

        assertEquals(1, countRows("pv_1"));

        // Crash — offset never acknowledged, Kafka will replay from last committed offset.
        // Simulate restart: fresh in-memory state, same durable SQLite connection.
        JoinEngine restarted = new JoinEngine(
                new ClickStateStore(), new WatermarkTracker(2, 30),
                outputSink, lateEventSink, new SimpleMeterRegistry());

        // Replay the exact same events from Kafka
        restarted.processClick(click("user_1", "click_1", "campaign_A", 5, 0));
        restarted.processPageView(pageView("user_1", "pv_1", "/product1", 10, 0));
        restarted.processClick(click("user_1", "click_advance", "campaign_X", 15, 0));

        // INSERT OR IGNORE: duplicate silently discarded — still exactly 1 row, correct attribution
        assertEquals(1, countRows("pv_1"));
        assertEquals("campaign_A", queryOutput("pv_1").getAttributedCampaignId());
    }

    @Test
    void latePageViewIsDropped() throws Exception {
        // Advance watermark to +30min via a click
        joinEngine.processClick(click("user_x", "click_x", "campaign_X", 30, 0));

        // Page view at +10min is now too late: watermark(30) - lateness(2) = 28min, pv at 10min < 28min
        joinEngine.processPageView(pageView("user_x", "pv_late", "/late", 10, 0));

        assertTrue(isInLateEvents("pv_late"));
        assertNull(queryOutput("pv_late"));
    }

    @Test
    void evictionCutoffUsesMinWatermarkAcrossPartitions() throws Exception {
        // p0: click_a at +20, pv_a at +50 (safe-emit=+52, watermark p0=+48 conservative)
        // p1: click_x at +60 (watermark p1=+60)
        // min(+48, +60) = +48 → cutoff = 48 - 30 - 2 = +16min → click_a(+20) retained
        // If max (+60) were used: cutoff = +28 → click_a wrongly evicted → null attribution

        joinEngine.processClick(click("user_a", "click_a", "campaign_A", 20, 0));
        joinEngine.processPageView(pageView("user_a", "pv_a", "/product_a", 50, 0));
        joinEngine.processClick(click("user_x", "click_x", "campaign_X", 60, 1));

        // pv_a not yet emitted (p0 watermark 48 < safe-emit 52)
        assertNull(queryOutput("pv_a"));

        // evictOldClicks: cutoff=+16, click_a(+20) survives; idle flush emits pv_a via Instant.now()
        joinEngine.evictOldClicks();

        AttributedPageView result = queryOutput("pv_a");
        assertNotNull(result);
        assertEquals("campaign_A", result.getAttributedCampaignId());
        assertEquals("click_a", result.getAttributedClickId());
    }
}