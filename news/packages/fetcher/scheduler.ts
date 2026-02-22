/**
 * Smart feed scheduling using a Poisson publish-rate model.
 *
 * Each feed has a publish rate λ (items/hour), estimated from the inter-arrival
 * times of the items we've seen. We then schedule the next fetch so we're
 * unlikely to miss items while not hammering low-activity feeds.
 *
 * Algorithm:
 *   1. From the N most-recent item timestamps, compute pairwise gaps (hours).
 *   2. λ = (N - 1) / sum(gaps)  — MLE of Poisson rate.
 *   3. Expected wait until next item = 1/λ hours.
 *   4. We fetch slightly early: nextFetch = now + (1/λ) * LEAD_FACTOR
 *      where LEAD_FACTOR < 1 ensures we don't miss the next item.
 *   5. Hard-clamp: [MIN_INTERVAL_HOURS, MAX_INTERVAL_HOURS].
 *
 * For feeds with unknown rate (new feeds or < 2 items): use DEFAULT_INTERVAL.
 *
 * The estimate is blended with the previous λ using exponential smoothing so
 * short bursts don't cause thrashing.
 */

/** Fraction of the expected inter-arrival time to use as the poll interval.
 *  0.6 means we fetch at 60% of the expected gap — giving us a head-start. */
const LEAD_FACTOR = 0.6;

/** Smoothing factor for updating λ: 0 = ignore new data, 1 = no memory. */
const ALPHA = 0.3;

/** Clamp bounds */
const MIN_INTERVAL_HOURS = 0.25;   // never faster than every 15 min
const MAX_INTERVAL_HOURS = 24;     // never slower than once a day

/** Used when we have no rate estimate yet */
const DEFAULT_INTERVAL_HOURS = 1;

/** How many recent items to use for the rate fit */
const SAMPLE_SIZE = 20;

/**
 * Given a list of item publish timestamps (any order) and the feed's current
 * known λ (may be null), return { nextFetchAt, newRate }.
 */
export function computeNextFetch(
    publishedTimestamps: (Date | null | undefined)[],
    currentRatePerHour: number | null | undefined,
): { nextFetchAt: Date; newRatePerHour: number | null } {
    const now = new Date();

    const validDates = publishedTimestamps
        .filter((d): d is Date => d instanceof Date && !isNaN(d.getTime()))
        .sort((a, b) => a.getTime() - b.getTime());  // ascending

    // Need at least 2 timestamps to compute a rate
    if (validDates.length < 2) {
        const intervalHours = DEFAULT_INTERVAL_HOURS;
        return {
            nextFetchAt: addHours(now, intervalHours),
            newRatePerHour: currentRatePerHour ?? null,
        };
    }

    // Use the most recent SAMPLE_SIZE items
    const sample = validDates.slice(-SAMPLE_SIZE);

    // Compute gaps in hours between consecutive items
    const gaps: number[] = [];
    for (let i = 1; i < sample.length; i++) {
        const gapHours = (sample[i]!.getTime() - sample[i - 1]!.getTime()) / 3_600_000;
        if (gapHours > 0) gaps.push(gapHours);
    }

    if (!gaps.length) {
        return {
            nextFetchAt: addHours(now, DEFAULT_INTERVAL_HOURS),
            newRatePerHour: currentRatePerHour ?? null,
        };
    }

    // MLE estimate of λ for Poisson process
    const sumGaps = gaps.reduce((a, b) => a + b, 0);
    const observedRate = gaps.length / sumGaps;  // items/hour

    // Exponential smoothing with previous estimate
    const smoothedRate = currentRatePerHour != null
        ? ALPHA * observedRate + (1 - ALPHA) * currentRatePerHour
        : observedRate;

    // Expected inter-arrival time in hours
    const expectedGapHours = 1 / smoothedRate;

    // Poll at LEAD_FACTOR of the expected gap, clamped
    const pollIntervalHours = clamp(
        expectedGapHours * LEAD_FACTOR,
        MIN_INTERVAL_HOURS,
        MAX_INTERVAL_HOURS,
    );

    return {
        nextFetchAt: addHours(now, pollIntervalHours),
        newRatePerHour: smoothedRate,
    };
}

function addHours(date: Date, hours: number): Date {
    return new Date(date.getTime() + hours * 3_600_000);
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}