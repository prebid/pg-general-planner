package org.prebid.pg.gp.server.services.algotest;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.services.HostBasedTokenReallocation;
import org.prebid.pg.gp.server.services.HostReallocationService;
import org.prebid.pg.gp.server.services.StatsCache;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;

import java.time.Instant;

/**
 * A service to do the host-based token reallocation in the simulation environment.
 */
public class HostAllocationServiceAlgoTest extends HostReallocationService {
    private static final Logger logger = LoggerFactory.getLogger(HostAllocationServiceAlgoTest.class);

    public HostAllocationServiceAlgoTest(
            Vertx vertx,
            HostReallocationConfiguration hostReallocationConfiguration,
            int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredPlannerDataAccessClient,
            HostBasedTokenReallocation hostBasedReallocation,
            Shutdown shutdown,
            StatsCache statsCache
    ) {
        super(
                vertx,
                hostReallocationConfiguration,
                pbsMaxIdlePeriodInSeconds,
                circuitBreakerSecuredPlannerDataAccessClient,
                hostBasedReallocation,
                new Metrics(new MetricRegistry()),
                shutdown,
                statsCache
        );
    }

    public Future<Void> doControlledCalculate(Instant endTime) {
        logger.info("doControlledCalculate for {0}", endTime);
        return calculate(endTime);
    }

    @Override
    public void initialize() {
        // do nothing
    }

}
