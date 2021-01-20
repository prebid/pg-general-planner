package org.prebid.pg.gp.server.breaker;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.spring.config.app.CircuitBreakerConfiguration;

import java.util.Objects;

/**
 * A {@code CircuitBreaker} for general planner.
 */
public class PlannerCircuitBreaker {

    private static final Logger logger = LoggerFactory.getLogger(PlannerCircuitBreaker.class);

    private final CircuitBreaker breaker;

    public PlannerCircuitBreaker(String name, Vertx vertx, CircuitBreakerConfiguration circuitBreakerConfiguration) {
        CircuitBreakerOptions circuitBreakerOptions = new CircuitBreakerOptions();
        circuitBreakerOptions.setMaxFailures(circuitBreakerConfiguration.getOpeningThreshold())
                .setResetTimeout((long) circuitBreakerConfiguration.getClosingIntervalSec() * 1000);

        breaker = CircuitBreaker
                .create(Objects.requireNonNull(name), Objects.requireNonNull(vertx), circuitBreakerOptions);

        logger.info(
                "Created PlannerCircuitBreaker {0}, with options={1}",
                breaker.name(),
                circuitBreakerOptions.toJson());
    }

    /**
     * Sets the given {@code handler} as the open handler of this circuit breaker.
     *
     * @param handler the handler
     * @return this circuit breaker
     */
    public PlannerCircuitBreaker openHandler(Handler<Void> handler) {
        breaker.openHandler(handler);
        return this;
    }

    /**
     * Sets the given {@code handler} as the half open handler of this circuit breaker.
     *
     * @param handler the handler
     * @return this circuit breaker
     */
    public PlannerCircuitBreaker halfOpenHandler(Handler<Void> handler) {
        breaker.halfOpenHandler(handler);
        return this;
    }

    /**
     * Sets the given {@code handler} as the close handler of this circuit breaker.
     *
     * @param handler the handler
     * @return this circuit breaker
     */
    public PlannerCircuitBreaker closeHandler(Handler<Void> handler) {
        breaker.closeHandler(handler);
        return this;
    }

    /**
     * Executes the passed in {@code command}.
     *
     * @param command the command to be executed
     * @param <T> type parameter
     * @return a future of T
     */
    public <T> Future<T> executeCommand(Handler<Future<T>> command) {
        return breaker.execute(future -> execute(command, future));
    }

    private <T> void execute(Handler<Future<T>> command, Future<T> future) {
        final Future<T> passedFuture = Future.future();
        command.handle(passedFuture);
        passedFuture
                .compose(response -> succeedBreaker(response, future))
                .recover(exception -> failBreaker(exception, future));
    }

    private <T> Future<T> succeedBreaker(T result, Future<T> future) {
        future.complete(result);
        return future;
    }

    private <T> Future<T> failBreaker(Throwable exception, Future<T> future) {
        future.fail(exception);
        return future;
    }

    public CircuitBreaker getBreaker() {
        return breaker;
    }

}
