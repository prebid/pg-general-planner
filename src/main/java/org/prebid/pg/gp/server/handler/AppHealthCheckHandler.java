package org.prebid.pg.gp.server.handler;

import io.vertx.circuitbreaker.CircuitBreakerState;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.RoutingContext;

/**
 * A handler for health check request.
 */
public class AppHealthCheckHandler implements Handler<RoutingContext> {

    private final HealthChecks healthChecks;

    public AppHealthCheckHandler(Vertx vertx) {
        this.healthChecks = HealthChecks.create(vertx);
    }

    /**
     * Registers the given {@code dataAccessClient} for health check.
     *
     * @param dataAccessClient the database access client to register for health check
     */
    public void register(CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient) {
        healthChecks.register("GeneralPlanner_DatabaseHealthCheck", 1000,
                future -> dataAccessClient.getPlannerDataAccessClient()
                        .connect()
                        .setHandler(ar -> {
                            if (ar.failed()) {
                                future.fail(ar.cause());
                            } else {
                                ar.result().close();
                                future.complete(Status.OK());
                            }
                        }));

        healthChecks.register("GeneralPlanner_DatabaseCircuitBreakerHealthCheck", 1000, future -> {
            CircuitBreakerState circuitBreakerState = dataAccessClient.getPlannerCircuitBreaker().getBreaker().state();
            if (circuitBreakerState.equals(CircuitBreakerState.CLOSED)) {
                future.complete(Status.OK());
            } else {
                future.fail("Circuit breaker is in " + circuitBreakerState + " state");
            }
        });
    }

    /**
     * Do nothing in this implementation.
     *
     * @param context context of request and response
     */
    @Override
    public void handle(RoutingContext context) {
        // do nothing
    }

    public HealthChecks getHealthChecks() {
        return healthChecks;
    }
}
