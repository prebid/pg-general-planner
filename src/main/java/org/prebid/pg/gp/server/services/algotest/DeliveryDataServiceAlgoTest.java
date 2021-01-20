package org.prebid.pg.gp.server.services.algotest;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredDeliveryDataHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.services.DeliveryDataService;
import org.prebid.pg.gp.server.services.StatsCache;
import org.prebid.pg.gp.server.spring.config.app.DeliveryDataConfiguration;

/**
 * A service to refresh line item delivery stats data in the simulation environment.
 */
public class DeliveryDataServiceAlgoTest extends DeliveryDataService {
    private static final Logger logger = LoggerFactory.getLogger(DeliveryDataServiceAlgoTest.class);

    public DeliveryDataServiceAlgoTest(
            Vertx vertx,
            DeliveryDataConfiguration deliveryDataConfiguration,
            int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredPlannerDataAccessClient,
            CircuitBreakerSecuredDeliveryDataHttpClient circuitBreakerSecuredDeliveryDataHttpClient,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertHttpClient,
            StatsCache statsCache,
            int startTimeInPastSec
    ) {
        super(
                vertx,
                deliveryDataConfiguration,
                pbsMaxIdlePeriodInSeconds,
                circuitBreakerSecuredPlannerDataAccessClient,
                circuitBreakerSecuredDeliveryDataHttpClient,
                metrics,
                adminTracer,
                shutdown,
                alertHttpClient,
                statsCache
        );
        setStartTimeInPastSec(startTimeInPastSec);
    }

    @Override
    public void initialize() {
        // do nothing
    }

    public Future<Void> doControlledRefreshDeliveryData(String simTime) {
        logger.info("doControlledRefreshDeliveryData");
        return refreshDeliveryData(simTime);
    }

}
