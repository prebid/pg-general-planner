package org.prebid.pg.gp.server.handler.algotest;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.prebid.pg.gp.server.services.algotest.PlannerAdapterServicesAlgoTest;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A handler for the request of refresh line items delivery plans when executing in the simulation environment.
 */
public class PlannerAdaptersUpdateAlgoTestHandler implements Handler<RoutingContext> {
    private static final Logger logger = LoggerFactory.getLogger(PlannerAdaptersUpdateAlgoTestHandler.class);

    private static final String PG_SIM_TIMESTAMP_HEADER = "pg-sim-timestamp";

    private final PlannerAdapterServicesAlgoTest adapterServices;

    public PlannerAdaptersUpdateAlgoTestHandler(PlannerAdapterServicesAlgoTest adapterServices) {
        this.adapterServices = Objects.requireNonNull(adapterServices);
    }

    @Override
    public void handle(RoutingContext context) {
        logger.info("algotest starts updatePlans.");

        String simTime = context.request().getHeader(PG_SIM_TIMESTAMP_HEADER);
        if (StringUtils.isEmpty(simTime)) {
            logger.error("algotest updatePlan request missing {} header.", PG_SIM_TIMESTAMP_HEADER);
            context.response().setStatusCode(400).end();
            return;
        }

        List<Future> futures = adapterServices.getPlannerAdapterServiceList().stream()
                .map(adapterService -> adapterService.refreshPlans(simTime))
                .collect(Collectors.toList());
        CompositeFuture.all(futures)
                .setHandler(ar -> {
                    logger.info("algotest updatePlans: {0}", ar.succeeded() ? "succeeded" : "failed");
                    context.response().setStatusCode(ar.succeeded() ? 200 : 500).end();
                });
    }
}
