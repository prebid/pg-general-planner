package org.prebid.pg.gp.server.handler;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerState;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.SQLConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.jdbc.PlannerDataAccessClient;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class AppHealthCheckHandlerTest {

    @Test
    void shouldRegisterForPlannerDataAccessClient() {
        CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock =
                mock(CircuitBreakerSecuredPlannerDataAccessClient.class);
        Vertx vertx = Vertx.vertx();
        AppHealthCheckHandler handler = new AppHealthCheckHandler(vertx);

        PlannerDataAccessClient plannerClinetMock = mock(PlannerDataAccessClient.class);
        SQLConnection cnnMock = mock(SQLConnection.class);
        given(plannerClinetMock.connect()).willReturn(Future.succeededFuture(cnnMock));
        PlannerCircuitBreaker breakMock = mock(PlannerCircuitBreaker.class);
        CircuitBreaker circuitBreakerMock = mock(CircuitBreaker.class);
        given(breakMock.getBreaker()).willReturn(circuitBreakerMock);
        given(circuitBreakerMock.state()).willReturn(CircuitBreakerState.CLOSED);
        given(dataAccessClientMock.getPlannerCircuitBreaker()).willReturn(breakMock);

        given(dataAccessClientMock.getPlannerDataAccessClient()).willReturn(plannerClinetMock);

        handler.register(dataAccessClientMock);

        handler.getHealthChecks().invoke("GeneralPlanner_DatabaseHealthCheck", rs -> {});
        handler.getHealthChecks().invoke("GeneralPlanner_DatabaseCircuitBreakerHealthCheck", rs -> {});

        verify(dataAccessClientMock).getPlannerDataAccessClient();
    }

    @Test
    void shouldRegisterForPlannerDataAccessClientFailureCases() {
        CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock =
                mock(CircuitBreakerSecuredPlannerDataAccessClient.class);
        Vertx vertx = Vertx.vertx();
        AppHealthCheckHandler handler = new AppHealthCheckHandler(vertx);

        PlannerDataAccessClient plannerClinetMock = mock(PlannerDataAccessClient.class);
        SQLConnection cnnMock = mock(SQLConnection.class);
        given(plannerClinetMock.connect()).willReturn(Future.failedFuture(new IllegalStateException()));
        PlannerCircuitBreaker breakMock = mock(PlannerCircuitBreaker.class);
        CircuitBreaker circuitBreakerMock = mock(CircuitBreaker.class);
        given(breakMock.getBreaker()).willReturn(circuitBreakerMock);
        given(circuitBreakerMock.state()).willReturn(CircuitBreakerState.HALF_OPEN);
        given(dataAccessClientMock.getPlannerCircuitBreaker()).willReturn(breakMock);

        given(dataAccessClientMock.getPlannerDataAccessClient()).willReturn(plannerClinetMock);

        handler.register(dataAccessClientMock);

        handler.getHealthChecks().invoke("GeneralPlanner_DatabaseHealthCheck", rs -> {});
        handler.getHealthChecks().invoke("GeneralPlanner_DatabaseCircuitBreakerHealthCheck", rs -> {});

        verify(dataAccessClientMock).getPlannerDataAccessClient();
    }

}
