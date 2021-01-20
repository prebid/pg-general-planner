package org.prebid.pg.gp.server.metric;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class MetricsTest {

    @Test
    public void shouldIncCounter() {
        MetricRegistry registry = new MetricRegistry();
        Metrics metrics = new Metrics(registry);
        String name = "foo";
        metrics.incCounter(name);
        assertThat(registry.counter(name).getCount(), equalTo(1L));
    }
}
