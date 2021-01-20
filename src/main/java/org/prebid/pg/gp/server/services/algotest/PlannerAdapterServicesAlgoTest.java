package org.prebid.pg.gp.server.services.algotest;

import io.vertx.core.Vertx;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.services.PlannerAdapterService;
import org.prebid.pg.gp.server.services.PlannerAdapterServices;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations;

import java.util.Map;

public class PlannerAdapterServicesAlgoTest extends PlannerAdapterServices {

    public PlannerAdapterServicesAlgoTest(Vertx vertx) {
        super(vertx);
    }

    public void initialize(
            String hostName,
            PlannerAdapterConfigurations paCfgs,
            CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient,
            Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> plannerAdapterHttpClients,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient,
            int simFuturePlanHours) {
        super.initialize(hostName,
                paCfgs,
                dataAccessClient,
                plannerAdapterHttpClients,
                metrics,
                adminTracer,
                shutdown,
                alertProxyHttpClient);
        for (PlannerAdapterService service : getPlannerAdapterServiceList()) {
            service.setFuturePlanHours(simFuturePlanHours);
        }

    }
}
