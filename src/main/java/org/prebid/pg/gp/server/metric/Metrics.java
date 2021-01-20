package org.prebid.pg.gp.server.metric;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 * A client to provide metrics service for General Planner.
 */
public class Metrics {

    private final MetricRegistry metricRegistry;

    public Metrics(MetricRegistry metricRegistry) {
        this.metricRegistry = java.util.Objects.requireNonNull(metricRegistry);
    }

    public void incCounter(String metricName) {
        incCounter(metricName, 1);
    }

    public void incCounter(String metricName, long value) {
        metricRegistry.counter(metricName, ResettingCounter::new).inc(value);
    }

    public void updateTimer(String metricName, long millis) {
        metricRegistry.timer(metricName).update(millis, TimeUnit.MILLISECONDS);
    }
}

class ResettingCounter extends Counter {

    @Override
    public long getCount() {
        final long count = super.getCount();
        dec(count);
        return count;
    }
}
