package org.prebid.pg.gp.server.breaker;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;
import org.prebid.pg.gp.server.spring.config.app.CircuitBreakerConfiguration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.isA;

public class CircuitBreakerSecuredClientTest {

    @Test
    void shouldSetupProperly() {
        CircuitBreakerConfiguration config = new CircuitBreakerConfiguration();
        config.setClosingIntervalSec(1);
        config.setOpeningThreshold(2);
        PlannerCircuitBreaker plannerCircuitBreaker = new PlannerCircuitBreaker("foo", Vertx.vertx(), config);
        CircuitBreakerSecuredClient client = new CircuitBreakerSecuredClient(plannerCircuitBreaker);
        assertThat(client, isA(CircuitBreakerSecuredClient.class));
    }
}
