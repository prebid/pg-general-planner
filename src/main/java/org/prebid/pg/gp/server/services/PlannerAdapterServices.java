package org.prebid.pg.gp.server.services;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations.PlannerAdapterConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A service to create and initialize {@link PlannerAdapterService}s to retrieve line items information
 * from Planner Adapters periodically.
 */
public class PlannerAdapterServices {

    private static final Logger logger = LoggerFactory.getLogger(PlannerAdapterServices.class);

    private final Vertx vertx;

    private final List<PlannerAdapterService> plannerAdapterServiceList = new ArrayList<>();

    public PlannerAdapterServices(Vertx vertx) {
        this.vertx = Objects.requireNonNull(vertx);
    }

    /**
     * Creates and initializes {@link PlannerAdapterService}s to retrieve line items information
     * from Planner Adapters periodically.
     *
     * @param hostName id of the host that this application deployed on
     * @param paCfgs configuration of planner adapters
     * @param dataAccessClient A client for database access
     * @param plannerAdapterHttpClients http client for access remote planner adapters
     * @param metrics a facade to access metrics service
     * @param adminTracer an administration tracer
     * @param shutdown server shutdown status object
     * @param alertProxyHttpClient a http client to send system alerts to
     */
    public void initialize(
            String hostName,
            PlannerAdapterConfigurations paCfgs,
            CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient,
            Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> plannerAdapterHttpClients,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient
    ) {
        Objects.requireNonNull(hostName);
        Objects.requireNonNull(paCfgs);
        Objects.requireNonNull(dataAccessClient);
        Objects.requireNonNull(plannerAdapterHttpClients);
        Objects.requireNonNull(metrics);
        Objects.requireNonNull(adminTracer);
        Objects.requireNonNull(shutdown);
        Objects.requireNonNull(alertProxyHttpClient);

        for (PlannerAdapterConfiguration paCfg : paCfgs.getPlanners()) {
            PlannerAdapterService service = new PlannerAdapterService(
                    vertx,
                    hostName,
                    paCfg,
                    dataAccessClient,
                    plannerAdapterHttpClients.get(paCfg.getName()),
                    paCfgs.getDbStoreBatchSize(),
                    metrics,
                    adminTracer,
                    shutdown,
                    alertProxyHttpClient
            );
            plannerAdapterServiceList.add(service);
        }
        logger.info("Created {0} Planner Adapter Services", plannerAdapterServiceList.size());

        for (PlannerAdapterService plannerAdapterService : plannerAdapterServiceList) {
            plannerAdapterService.initialize();
        }
    }

    public List<PlannerAdapterService> getPlannerAdapterServiceList() {
        return plannerAdapterServiceList;
    }
}
