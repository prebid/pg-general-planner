package org.prebid.pg.gp.server.http;

import org.prebid.pg.gp.server.breaker.CircuitBreakerSecuredClient;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;

/**
 * A {@link PlannerAdapterHttpClient} guarded by the circuit breaker.
 */
public class CircuitBreakerSecuredPlannerAdapterHttpClient extends CircuitBreakerSecuredClient {

    private final PlannerAdapterHttpClient plannerAdapterHttpClient;

    public CircuitBreakerSecuredPlannerAdapterHttpClient(
            PlannerAdapterHttpClient plannerAdapterHttpClient,
            PlannerCircuitBreaker plannerCircuitBreaker) {
        super(plannerCircuitBreaker);
        this.plannerAdapterHttpClient = plannerAdapterHttpClient;
    }

    /**
     * Sends http request to the planner adapter to retrieve line item delivery plans.
     *
     * @param method the http method of the request
     * @param url the planner adapter resource url
     * @param username the username
     * @param password the password
     * @param simTime the current time of the simulation environment
     * @return a future of {@link HttpResponseContainer}
     */
    public Future<HttpResponseContainer> request(HttpMethod method, String url, String username, String password,
            String simTime) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerAdapterHttpClient.request(method, url, username, password, simTime).setHandler(future)
        );
    }

}

