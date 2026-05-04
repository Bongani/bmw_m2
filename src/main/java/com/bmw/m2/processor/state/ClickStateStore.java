package com.bmw.m2.processor.state;

import com.bmw.m2.processor.model.AdClickEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 * Think of it as a per-user history buffer that allows us to quickly find recent clicks for a user
 *
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 *
 */
@Slf4j
@Component
public class ClickStateStore {

    // Attribution window: clicks within last 30 minutes can be attributed
    private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    // Hint: Consider using ConcurrentHashMap and TreeSet for thread-safe, sorted storage
    private final ConcurrentHashMap<String, TreeSet<AdClickEvent>> clickStateHashMap = new ConcurrentHashMap<>();
    // - ConcurrentHashMap is Thread-safe reads and writes across different users without a global lock
    //  - Thread 0 processing partition 0 (user_1) and thread 1 processing partition 1 (user_2) never block each other
    //  - Each user's clicks are isolated
    //  TreeSet<AdClickEvent> — inner set, per user
    //  - Keeps clicks sorted by eventTime automatically — no manual sorting needed
    //  - Finding the latest click in the 30-min window is a single subSet() + last() call
    //  - Deduplicates by clickId if you define the comparator correctly
    //  The one catch: TreeSet itself is not thread-safe — we need to synchronize access to each user's TreeSet when adding clicks or evicting old clicks
    // For example, could evict while trying to write/read or events processing from different partitions
    // https://www.geeksforgeeks.org/java/reentrant-lock-in-java/
    private final ConcurrentHashMap<String, ReentrantLock> userLocks = new ConcurrentHashMap<>();
    /**
     * Add a click event to the state store.
     *
     * - Use locks for thread safety
     * - Store clicks sorted by event time (most recent first)
     * - Handle concurrent access properly
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click) {
        String userId = click.getUserId();

        // Get or create the lock for this user
        if (!userLocks.containsKey(userId)) {
            userLocks.put(userId, new ReentrantLock());
        }
        ReentrantLock lock = userLocks.get(userId);

        // Get or create the sorted click set for this user in 2 levels
        // level 1-> sort by click eventTime
        // level 2-> sort by id if 2 events occur at the same time
        if (!clickStateHashMap.containsKey(userId)) {
            Comparator<AdClickEvent> byEventTimeThenClickId = Comparator
                    .comparing(AdClickEvent::getEventTime)
                    .thenComparing(AdClickEvent::getClickId);
            clickStateHashMap.put(userId, new TreeSet<>(byEventTimeThenClickId));
        }
        TreeSet<AdClickEvent> userClicks = clickStateHashMap.get(userId);

        // Lock and add the click
        lock.lock();
        try {
            userClicks.add(click);
            log.debug("Added click {} for user {} at {}", click.getClickId(), userId, click.getEventTime());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find the most recent click for a user within the attribution window.
     *
     * - Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
     * - Return the most recent click within the window
     * - Return null if no click found
     *
     * @param userId the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        log.debug("Finding attributable click for user {} at time {}", userId, pageViewTime);
        TreeSet<AdClickEvent> userClicks = clickStateHashMap.get(userId);

        if (userClicks == null || userClicks.isEmpty()) {
            log.debug("No clicks found for user {}", userId);
            return null;
        }

        ReentrantLock lock = userLocks.get(userId);
        lock.lock();
        try {
            // Calculate the start of the 30-minute attribution window
            Instant windowStart = pageViewTime.minus(ATTRIBUTION_WINDOW);

            // Create a dummy lower-bound key — empty clickId sorts before any real clickId
            AdClickEvent lowerBound = AdClickEvent.builder().eventTime(windowStart).clickId("").build();

            // Create a dummy upper-bound key — "~" (ASCII 126) sorts after any realistic clickId
            AdClickEvent upperBound = AdClickEvent.builder().eventTime(pageViewTime).clickId("~").build();

            // Get all clicks within the window [windowStart, pageViewTime]
            TreeSet<AdClickEvent> clicksInWindow = new TreeSet<>(userClicks.subSet(lowerBound, true, upperBound, true));

            if (clicksInWindow.isEmpty()) {
                log.debug("No clicks in attribution window for user {} at {}", userId, pageViewTime);
                return null;
            }

            // Return the latest click in the window
            AdClickEvent latestClick = clicksInWindow.last();
            log.debug("Found attributable click {} for user {} at {}", latestClick.getClickId(), userId, latestClick.getEventTime());
            return latestClick;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     *
     * - Remove clicks older than the cutoff time
     * - Clean up empty user entries
     * - Return count of evicted clicks
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime) {
        log.debug("Evicting clicks older than {}", cutoffTime);
        int evictedCount = 0;

        //  Iterate over every user in clickStateHashMap
        for (String userId : clickStateHashMap.keySet()) {
            ReentrantLock lock = userLocks.get(userId);
            if (lock == null) {
                continue;
            }

            lock.lock();
            try {
                TreeSet<AdClickEvent> userClicks = clickStateHashMap.get(userId);
                if (userClicks == null) {
                    continue;
                }

                // headSet returns all clicks with eventTime strictly before the cutoff
                AdClickEvent cutoffKey = AdClickEvent.builder().eventTime(cutoffTime).clickId("").build();
                TreeSet<AdClickEvent> oldClicks = new TreeSet<>(userClicks.headSet(cutoffKey, false));

                // Count how many are being removed
                int evictedForUser = oldClicks.size();
                userClicks.removeAll(oldClicks);
                evictedCount += evictedForUser;

                // Clean up empty user entries to free memory
                if (userClicks.isEmpty()) {
                    clickStateHashMap.remove(userId);
                    userLocks.remove(userId);
                    log.debug("Removed empty state for user {}", userId);
                } else {
                    log.debug("Evicted {} clicks for user {}", evictedForUser, userId);
                }

            } finally {
                lock.unlock();
            }
        }

        log.info("Eviction complete: removed {} clicks older than {}", evictedCount, cutoffTime);
        return evictedCount;
    }

    /**
     * Get the total number of clicks currently in state.
     *
     * @return total click count across all users
     */
    public long getTotalClickCount() {
        long total = 0;
        for (TreeSet<AdClickEvent> userClicks : clickStateHashMap.values()) {
            total += userClicks.size();
        }
        return total;
    }
}
