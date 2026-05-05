package com.bmw.m2.processor.state;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class WatermarkTrackerTest {

    private WatermarkTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new WatermarkTracker(2, 30, new SimpleMeterRegistry());
    }

    @Test
    void initialWatermarkIsMin() {
        assertEquals(Instant.MIN, tracker.getWatermark(0));
    }

    @Test
    void watermarkAdvancesWithNewerEvent() {
        Instant t1 = Instant.parse("2024-01-01T12:00:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:05:00Z");

        tracker.updateWatermark(0, t1);
        tracker.updateWatermark(0, t2);

        assertEquals(t2, tracker.getWatermark(0));
    }

    @Test
    void watermarkDoesNotGoBackward() {
        Instant t1 = Instant.parse("2024-01-01T12:05:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:00:00Z"); // earlier

        tracker.updateWatermark(0, t1);
        tracker.updateWatermark(0, t2);

        assertEquals(t1, tracker.getWatermark(0));
    }

    @Test
    void watermarksTrackedPerPartition() {
        Instant t0 = Instant.parse("2024-01-01T12:10:00Z");
        Instant t1 = Instant.parse("2024-01-01T12:20:00Z");

        tracker.updateWatermark(0, t0);
        tracker.updateWatermark(1, t1);

        assertEquals(t0, tracker.getWatermark(0));
        assertEquals(t1, tracker.getWatermark(1));
    }

    @Test
    void isTooLateReturnsFalseWithNoWatermark() {
        Instant event = Instant.parse("2024-01-01T12:00:00Z");
        assertFalse(tracker.isTooLate(0, event));
    }

    @Test
    void isTooLateReturnsTrueForEventBeyondLateness() {
        // watermark at 12:10, lateness = 2 min, cutoff = 12:08
        // event at 12:07 → too late
        Instant watermark = Instant.parse("2024-01-01T12:10:00Z");
        Instant lateEvent = Instant.parse("2024-01-01T12:07:00Z");

        tracker.updateWatermark(0, watermark);

        assertTrue(tracker.isTooLate(0, lateEvent));
    }

    @Test
    void isTooLateReturnsFalseForEventWithinLateness() {
        // watermark at 12:10, lateness = 2 min, cutoff = 12:08
        // event at 12:09 → within lateness
        Instant watermark = Instant.parse("2024-01-01T12:10:00Z");
        Instant okEvent = Instant.parse("2024-01-01T12:09:00Z");

        tracker.updateWatermark(0, watermark);

        assertFalse(tracker.isTooLate(0, okEvent));
    }

    @Test
    void getMinWatermarkReturnsMinAcrossPartitions() {
        Instant t0 = Instant.parse("2024-01-01T12:05:00Z");
        Instant t1 = Instant.parse("2024-01-01T12:15:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:10:00Z");

        tracker.updateWatermark(0, t0);
        tracker.updateWatermark(1, t1);
        tracker.updateWatermark(2, t2);

        assertEquals(t0, tracker.getMinWatermark());
    }

    @Test
    void getMinWatermarkReturnsMinWhenEmpty() {
        assertEquals(Instant.MIN, tracker.getMinWatermark());
    }
}