package org.prebid.pg.gp.server.services;

import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A cache for {@link DeliveryTokenSpendSummary}s retrieved from stats server.
 */
public class StatsCache {

    private final AtomicReference<List<DeliveryTokenSpendSummary>> ref;

    public StatsCache() {
        ref = new AtomicReference<>(Collections.unmodifiableList(Collections.emptyList()));
    }

    /**
     * refresh the whole cache with the given items.
     *
     * @param items list of {@code DeliveryTokenSpendSummary}s
     */
    public void set(List<DeliveryTokenSpendSummary> items) {
        Objects.requireNonNull(items);
        ref.set(Collections.unmodifiableList(items));
    }

    /**
     * Gets all the {@code DeliveryTokenSpendSummary}s in the cache.
     *
     * @return list of {@code DeliveryTokenSpendSummary}s in the cache
     */
    public List<DeliveryTokenSpendSummary> get() {
        return ref.get();
    }

}
