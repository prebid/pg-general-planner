package org.prebid.pg.gp.server.http;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.spring.config.app.CircuitBreakerConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ExtendWith(VertxExtension.class)
class CircuitBreakerSecuredPlannerAdapterHttpClientTest {

    private static Vertx vertx;

    private PlannerAdapterHttpClient plannerAdapterHttpClient;

    private HttpClientConfiguration httpClientConfiguration;

    private CircuitBreakerConfiguration circuitBreakerConfiguration;

    private CircuitBreakerSecuredPlannerAdapterHttpClient circuitBreakerSecuredPlannerAdapterHttpClient;

    @BeforeAll
    static void prepare() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void cleanUp() {
        vertx.close();
    }

    @BeforeEach
    void setUpBeforeEach() {
        plannerAdapterHttpClient = Mockito.mock(PlannerAdapterHttpClient.class);
        httpClientConfiguration = Mockito.mock(HttpClientConfiguration.class);
        circuitBreakerConfiguration = Mockito.mock(CircuitBreakerConfiguration.class);

        given(httpClientConfiguration.getCircuitBreaker()).willReturn(circuitBreakerConfiguration);
        PlannerCircuitBreaker breaker =
                new PlannerCircuitBreaker("gp-name-planner-adapter-cb", vertx, circuitBreakerConfiguration);

        circuitBreakerSecuredPlannerAdapterHttpClient =
                new CircuitBreakerSecuredPlannerAdapterHttpClient(plannerAdapterHttpClient, breaker);
    }

    @AfterEach
    void cleanUpAfterEach() {
    }

    @Test
    void shouldReturnHttpResponseOnSuccess(VertxTestContext context) {
        given(plannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().statusCode(200).body("content").build())
        );

        Future<HttpResponseContainer> httpResponseContainerFuture =
                circuitBreakerSecuredPlannerAdapterHttpClient.request(
                        HttpMethod.GET, "url", "username", "password", ""
                );

        httpResponseContainerFuture.setHandler(context.succeeding(httpResponseContainer -> {
            context.verify(() -> {
                assertThat(httpResponseContainer.getStatusCode(), equalTo(200));
                assertThat(httpResponseContainer.getBody(), equalTo("content"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnPlannerCircuitBreaker() {
        final PlannerCircuitBreaker plannerCircuitBreaker
                = circuitBreakerSecuredPlannerAdapterHttpClient.getPlannerCircuitBreaker();

        PlannerCircuitBreaker expected
                = new PlannerCircuitBreaker("gp-name-planner-adapter-cb", vertx, httpClientConfiguration.getCircuitBreaker());

        assertThat(plannerCircuitBreaker.getBreaker().name(), equalTo(expected.getBreaker().name()));
        assertThat(plannerCircuitBreaker.getBreaker().state(), equalTo(expected.getBreaker().state()));
    }
}