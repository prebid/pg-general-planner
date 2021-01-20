package org.prebid.pg.gp.server.http;

import org.prebid.pg.gp.server.breaker.CircuitBreakerSecuredClient;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;

/**
 * A {@link DeliveryDataHttpClient} guarded by the circuit breaker.
 */
public class CircuitBreakerSecuredDeliveryDataHttpClient extends CircuitBreakerSecuredClient {

    private final DeliveryDataHttpClient deliveryDataHttpClient;

    public CircuitBreakerSecuredDeliveryDataHttpClient(
            DeliveryDataHttpClient deliveryDataHttpClient,
            PlannerCircuitBreaker plannerCircuitBreaker) {
        super(plannerCircuitBreaker);
        this.deliveryDataHttpClient = deliveryDataHttpClient;
    }

    /**
     * Sends http request to the stats server to retrieve latest line item delivery stats information.
     *
     * @param method the http method of the request
     * @param url the stats server resource url
     * @param username the username
     * @param password the password
     * @param simTime the current time of the simulation environment
     * @return a future of {@link HttpResponseContainer}
     */
    public Future<HttpResponseContainer> request(HttpMethod method, String url, String username, String password,
            String simTime) {
        return plannerCircuitBreaker.executeCommand(
                future -> deliveryDataHttpClient.request(method, url, username, password, simTime).setHandler(future)
        );
    }

}
