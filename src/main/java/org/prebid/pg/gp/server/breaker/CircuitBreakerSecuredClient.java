package org.prebid.pg.gp.server.breaker;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * A {@link PlannerCircuitBreaker} with logging support.
 */
public class CircuitBreakerSecuredClient {

    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerSecuredClient.class);

    protected final PlannerCircuitBreaker plannerCircuitBreaker;

    public CircuitBreakerSecuredClient(PlannerCircuitBreaker plannerCircuitBreaker) {
        this.plannerCircuitBreaker = plannerCircuitBreaker;
        this.plannerCircuitBreaker
                .openHandler(ignored -> circuitOpened())
                .halfOpenHandler(ignored -> circuitHalfOpened())
                .closeHandler(ignored -> circuitClosed());
    }

    private void circuitOpened() {
        logger.warn("Circuit {0} opened", plannerCircuitBreaker.getBreaker().name());
    }

    private void circuitHalfOpened() {
        logger.warn("Circuit {0} is half-open, ready to try again", plannerCircuitBreaker.getBreaker().name());
    }

    private void circuitClosed() {
        logger.info("Circuit {0} is closed", plannerCircuitBreaker.getBreaker().name());
    }

    public PlannerCircuitBreaker getPlannerCircuitBreaker() {
        return plannerCircuitBreaker;
    }

}
