package org.prebid.pg.gp.server.http;

import com.codahale.metrics.MetricRegistry;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.spring.config.app.CircuitBreakerConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations.PlannerAdapterConfiguration;

import java.time.Instant;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

@ExtendWith(VertxExtension.class)
class PlannerAdapterHttpClientTest {

    private static Vertx vertx;

    private static PlannerAdapterHttpClient plannerAdapterHttpClient;

    private static WireMockServer wireMockServer;

    private String okRespBody = "Response body from Wiremock Server";

    private AdminTracer tracer;

    @BeforeAll
    static void prepare() throws Exception {
        vertx = Vertx.vertx();
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
        configureFor(wireMockServer.port());
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        wireMockServer.shutdown();
        vertx.close();
    }

    @BeforeEach
    void setUpBeforeEach() {
        CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();
        circuitBreakerConfiguration.setClosingIntervalSec(30);
        circuitBreakerConfiguration.setOpeningThreshold(10);
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setConnectTimeoutSec(30);
        httpClientConfiguration.setMaxPoolSize(5);
        httpClientConfiguration.setCircuitBreaker(circuitBreakerConfiguration);
        PlannerAdapterConfiguration adapterConfiguration = new PlannerAdapterConfiguration();
        adapterConfiguration.setTimeoutSec(10);
        Metrics metrics = new Metrics(new MetricRegistry());

        tracer = new AdminTracer();
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        tracer.setEnabled(true);
        tracer.setRaw(true);

        AlertProxyHttpClient alertHttpClientMock = mock(AlertProxyHttpClient.class);

        plannerAdapterHttpClient = new PlannerAdapterHttpClient(
                vertx, httpClientConfiguration, adapterConfiguration, metrics, tracer, alertHttpClientMock);
    }

    @AfterEach
    void cleanUpAfterEach() {
    }

    @Test
    void shouldReturnResponseBodyWithApplicationHeadersAndStatusCodeWhenTracerEnabled(VertxTestContext context) {
        shouldReturnResponseBodyWithApplicationHeadersAndStatusCode(context);
    }

    @Test
    void shouldReturnResponseBodyWithApplicationHeadersAndStatusCodeWhenTracerDisabled(VertxTestContext context) {
        tracer.setEnabled(false);
        shouldReturnResponseBodyWithApplicationHeadersAndStatusCode(context);
    }

    private void shouldReturnResponseBodyWithApplicationHeadersAndStatusCode(VertxTestContext context) {
        String hdrTag1 = "HDR-TAG-1";
        String hdrVal1 = "HDR-VAL-1";
        String hdrTag2 = "HDR-TAG-2";
        String hdrVal2 = "HDR-VAL-2";

        givenThat(get(urlEqualTo("/api/message"))
                .willReturn(
                        aResponse()
                                .withStatus(200)
                                .withHeader(hdrTag1, hdrVal1)
                                .withHeader(hdrTag2, hdrVal2)
                                .withBody(okRespBody)
                ));

        final Future<HttpResponseContainer> future = plannerAdapterHttpClient.request(
                HttpMethod.GET,"http://localhost:" + wireMockServer.port() + "/api/message", "rp", "rp_password", ""
        );

        future.setHandler(context.succeeding(httpResponseContainer -> {
            context.verify(() -> {
                assertThat(httpResponseContainer.getHeaders().get(hdrTag1), equalTo(hdrVal1));
                assertThat(httpResponseContainer.getHeaders().get(hdrTag2), equalTo(hdrVal2));
                assertThat(httpResponseContainer.getStatusCode(), equalTo(200));
                assertThat(httpResponseContainer.getBody(), equalTo(okRespBody));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionOnUnreachableURL(VertxTestContext context) {
        final Future<HttpResponseContainer> future = plannerAdapterHttpClient.request(
                HttpMethod.GET,"http://localhost:8082/api/message", "rp", "rp_password", ""
        );

        future.setHandler(context.failing(throwable -> {
            context.verify(() -> {
                assert(true);
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionOnNullURL(VertxTestContext context) {
        final Future<HttpResponseContainer> future = plannerAdapterHttpClient.request(
                HttpMethod.GET, null, "rp", "rp_password", ""
        );

        future.setHandler(context.failing(throwable -> {
            context.verify(() -> {
                assert(true);
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionOnEmptyURL(VertxTestContext context) {
        final Future<HttpResponseContainer> future = plannerAdapterHttpClient.request(
                HttpMethod.GET, "", "rp", "rp_password", ""
        );

        future.setHandler(context.failing(throwable -> {
            context.verify(() -> {
                assert(true);
                context.completeNow();
            });
        }));
    }
}