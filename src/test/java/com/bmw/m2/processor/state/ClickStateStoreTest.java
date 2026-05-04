package com.bmw.m2.processor.state;

import com.bmw.m2.processor.model.AdClickEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ClickStateStoreTest {

    private ClickStateStore store;

    private static final Instant BASE = Instant.parse("2026-01-01T12:00:00Z");

    @BeforeEach
    void setUp() {
        store = new ClickStateStore();
    }

    // helper to build a click event
    private AdClickEvent click(String userId, String clickId, String campaignId, Instant eventTime) {
        return AdClickEvent.builder()
                .userId(userId)
                .clickId(clickId)
                .campaignId(campaignId)
                .eventTime(eventTime)
                .build();
    }

    // --- addClick ---

    @Test
    void addClickStoresClick() {
        store.addClick(click("user_1", "click_1", "campaign_A", BASE.plus(5, ChronoUnit.MINUTES)));
        assertEquals(1, store.getTotalClickCount());
    }

    @Test
    void addClickMultipleUsersTrackedSeparately() {
        store.addClick(click("user_1", "click_1", "campaign_A", BASE.plus(5, ChronoUnit.MINUTES)));
        store.addClick(click("user_2", "click_2", "campaign_B", BASE.plus(10, ChronoUnit.MINUTES)));
        assertEquals(2, store.getTotalClickCount());
    }

    @Test
    void addClickSameUserMultipleClicks() {
        store.addClick(click("user_3", "click_3a", "campaign_C", BASE.plus(20, ChronoUnit.MINUTES)));
        store.addClick(click("user_3", "click_3b", "campaign_D", BASE.plus(25, ChronoUnit.MINUTES)));
        assertEquals(2, store.getTotalClickCount());
    }

    // --- findAttributableClick ---

    @Test
    void findAttributableClickNormalCase() {
        // user_1: click at BASE+5, page view at BASE+10 — within 30min window
        store.addClick(click("user_1", "click_1", "campaign_A", BASE.plus(5, ChronoUnit.MINUTES)));

        AdClickEvent result = store.findAttributableClick("user_1", BASE.plus(10, ChronoUnit.MINUTES));

        assertNotNull(result);
        assertEquals("click_1", result.getClickId());
        assertEquals("campaign_A", result.getCampaignId());
    }

    @Test
    void findAttributableClickPicksLatestWhenMultipleInWindow() {
        // user_3: two clicks in window — must pick the most recent one
        store.addClick(click("user_3", "click_3a", "campaign_C", BASE.plus(20, ChronoUnit.MINUTES)));
        store.addClick(click("user_3", "click_3b", "campaign_D", BASE.plus(25, ChronoUnit.MINUTES)));

        AdClickEvent result = store.findAttributableClick("user_3", BASE.plus(30, ChronoUnit.MINUTES));

        assertNotNull(result);
        assertEquals("click_3b", result.getClickId());
        assertEquals("campaign_D", result.getCampaignId());
    }

    @Test
    void findAttributableClickReturnsNullWhenClickOutsideWindow() {
        // user_4: click at BASE+35, page view at BASE+70 — 35 min gap, outside 30min window
        store.addClick(click("user_4", "click_4", "campaign_E", BASE.plus(35, ChronoUnit.MINUTES)));

        AdClickEvent result = store.findAttributableClick("user_4", BASE.plus(70, ChronoUnit.MINUTES));

        assertNull(result);
    }

    @Test
    void findAttributableClickReturnsNullWhenNoClicksForUser() {
        // user_6: page view with no prior clicks at all
        AdClickEvent result = store.findAttributableClick("user_6", BASE.plus(80, ChronoUnit.MINUTES));

        assertNull(result);
    }

    @Test
    void findAttributableClickWindowBoundaryIsInclusive() {
        // click exactly 30 minutes before page view — should be included
        store.addClick(click("user_1", "click_1", "campaign_A", BASE));

        AdClickEvent result = store.findAttributableClick("user_1", BASE.plus(30, ChronoUnit.MINUTES));

        assertNotNull(result);
        assertEquals("click_1", result.getClickId());
    }

    // --- evictOldClicks ---

    @Test
    void evictOldClicksRemovesClicksBeforeCutoff() {
        store.addClick(click("user_1", "click_old", "campaign_A", BASE));
        store.addClick(click("user_1", "click_new", "campaign_A", BASE.plus(10, ChronoUnit.MINUTES)));

        // cutoff at BASE+5: click_old (BASE) is before cutoff, click_new (BASE+10) is after
        int evicted = store.evictOldClicks(BASE.plus(5, ChronoUnit.MINUTES));

        assertEquals(1, evicted);
        assertEquals(1, store.getTotalClickCount());
    }

    @Test
    void evictOldClicksRemovesEmptyUserEntries() {
        store.addClick(click("user_1", "click_1", "campaign_A", BASE));

        // evict with cutoff after the only click
        store.evictOldClicks(BASE.plus(1, ChronoUnit.MINUTES));

        assertEquals(0, store.getTotalClickCount());
    }

    @Test
    void evictOldClicksReturnsZeroWhenNothingToEvict() {
        store.addClick(click("user_1", "click_1", "campaign_A", BASE.plus(20, ChronoUnit.MINUTES)));

        // cutoff is before all clicks — nothing should be evicted
        int evicted = store.evictOldClicks(BASE);

        assertEquals(0, evicted);
        assertEquals(1, store.getTotalClickCount());
    }

    // --- getTotalClickCount ---

    @Test
    void getTotalClickCountReturnsZeroInitially() {
        assertEquals(0, store.getTotalClickCount());
    }

    @Test
    void getTotalClickCountReflectsAllUsers() {
        store.addClick(click("user_1", "click_1", "campaign_A", BASE.plus(5, ChronoUnit.MINUTES)));
        store.addClick(click("user_2", "click_2", "campaign_B", BASE.plus(10, ChronoUnit.MINUTES)));
        store.addClick(click("user_3", "click_3a", "campaign_C", BASE.plus(20, ChronoUnit.MINUTES)));
        store.addClick(click("user_3", "click_3b", "campaign_D", BASE.plus(25, ChronoUnit.MINUTES)));

        assertEquals(4, store.getTotalClickCount());
    }

    // --- Tests to check thread safety on accessing the TreeSet of clickStateHashMap ---


    /**
     * The specific risk is the eviction scheduler thread running evictOldClicks() concurrently with consumer threads calling addClick()
     * Simulates : two consumer threads (one per partition) adding clicks concurrently while the @Scheduled eviction thread runs at the same time.
     * Catches ConcurrentModificationException on keySet() iteration and NullPointerException from
     * userLocks.get() returning null after eviction cleanup removes the user entry.
     * @throws Exception
     */
    @Test
    void concurrentAddAndEvictDoNotCorruptState() throws Exception {
        // high iteteration to have some kind of gurantee of concurrency, but not too high to cause test timeouts
        int numberOfEvents = 500;
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // Consumer thread 1 — partition 0, user_a
        Future<?> t1 = executor.submit(() -> {
            for (int i = 0; i < numberOfEvents; i++) {
                store.addClick(click("user_a", "click_a_" + i, "campaign_A",
                        BASE.plus(i, ChronoUnit.SECONDS)));
            }
        });

        // Consumer thread 2 — partition 1, user_b
        Future<?> t2 = executor.submit(() -> {
            for (int i = 0; i < numberOfEvents; i++) {
                store.addClick(click("user_b", "click_b_" + i, "campaign_B",
                        BASE.plus(i, ChronoUnit.SECONDS)));
            }
        });

        // Eviction thread — simulates @Scheduled(fixedRate=30000) running concurrently
        Future<?> t3 = executor.submit(() -> {
            for (int i = 0; i < 50; i++) {
                store.evictOldClicks(BASE.plus(i * 10L, ChronoUnit.SECONDS));
            }
        });

        assertDoesNotThrow(() -> {
            t1.get(5, TimeUnit.SECONDS);
            t2.get(5, TimeUnit.SECONDS);
            t3.get(5, TimeUnit.SECONDS);
        });

        executor.shutdown();
        assertTrue(store.getTotalClickCount() >= 0);
    }
}