package org.prebid.pg.gp.server.services;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.TracerFilters;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class PlannerAdapterServiceTest {

    private static Vertx vertx;

    private final String tsStr = "2019-07-18T18:54:46.000Z";

    private CircuitBreakerSecuredPlannerAdapterHttpClient circuitBreakerSecuredPlannerAdapterHttpClient;

    private CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredPlannerDataAccessClient;

    private PlannerAdapterService plannerAdapterService;

    private PlannerAdapterConfigurations.PlannerAdapterConfiguration plannerAdapterConfiguration;

    private ObjectMapper objectMapper = new ObjectMapper();

    private AdminTracer tracer;

    private AlertProxyHttpClient alertProxyHttpClient;

    private Shutdown shutdown;

    private String vendor = "vendor1";

    @BeforeAll
    static void prepare() {
        vertx = Vertx.vertx();
    }

    @BeforeEach
    void setUpBeforeEach() {
        plannerAdapterConfiguration = new PlannerAdapterConfigurations.PlannerAdapterConfiguration();
        plannerAdapterConfiguration.setName(vendor);
        plannerAdapterConfiguration.setEnabled(true);
        plannerAdapterConfiguration.setUsername("rp");
        plannerAdapterConfiguration.setPassword("rp_password");
        plannerAdapterConfiguration.setUrl("some.com");
        plannerAdapterConfiguration.setInitialDelaySec(1);
        plannerAdapterConfiguration.setRefreshPeriodSec(3);
        plannerAdapterConfiguration.setBidderCodePrefix("pg");

        circuitBreakerSecuredPlannerAdapterHttpClient = mock(CircuitBreakerSecuredPlannerAdapterHttpClient.class);
        circuitBreakerSecuredPlannerDataAccessClient = mock(CircuitBreakerSecuredPlannerDataAccessClient.class);
        alertProxyHttpClient = mock(AlertProxyHttpClient.class);

        shutdown = new Shutdown();

        tracer = new AdminTracer();
        TracerFilters filters = new TracerFilters();
        filters.setBidderCode(vendor.toUpperCase());
        filters.setAccountId("1001");
        filters.setVendor(vendor);
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        tracer.setFilters(filters);

        plannerAdapterService = new PlannerAdapterService(
                vertx, "MyMac", plannerAdapterConfiguration,
                circuitBreakerSecuredPlannerDataAccessClient, circuitBreakerSecuredPlannerAdapterHttpClient,
                10, new Metrics(new MetricRegistry()), tracer, shutdown, alertProxyHttpClient);

        objectMapper.findAndRegisterModules();
    }

    @AfterAll
    static void cleanUp() {
        vertx.close();
    }

    @Test
    void initialize() {
    }

    @Test
    void shouldNotRefreshDuringShutdown() {
        shutdown.setInitiating(true);
        plannerAdapterService.refreshPlans();
        verify(circuitBreakerSecuredPlannerAdapterHttpClient, times(0))
                .request(any(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void shouldRefreshPlansOnSunnyDay() throws Exception {
        final String baseDir = "planner-adapter-service/sunny-day-1";
        final String lineItemFileName = "plan-1.json";

        final URL url = Resources.getResource(String.format("%s/input/%s", baseDir, lineItemFileName));
        final File myFile = new File(url.toURI());
        final String content = FileUtils.readFileToString(myFile, "UTF-8");

        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body(content).statusCode(200).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET, buildUrl(), "rp", "rp_password", null);
        verify(circuitBreakerSecuredPlannerDataAccessClient).updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldRefreshPlansOnSunnyDayWithSinceTime() throws Exception {
        final String baseDir = "planner-adapter-service/sunny-day-1";
        final String lineItemFileName = "plan-1.json";

        final URL url = Resources.getResource(String.format("%s/input/%s", baseDir, lineItemFileName));
        final File myFile = new File(url.toURI());

        final String content = FileUtils.readFileToString(myFile, "UTF-8");

        String now = "2019-01-28T22:16:44.000Z";
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body(content).statusCode(200).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.setFuturePlanHours(1);
        plannerAdapterService.refreshPlans(now);

        String reqUrl = buildUrl() + "?since=2019-01-28T22:16:44.000Z&hours=1";
        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET, reqUrl, "rp", "rp_password", "2019-01-28T22:16:44.000Z");
        verify(circuitBreakerSecuredPlannerDataAccessClient).updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldNotDropLineItemWithEmptyPlan() throws Exception {
        tracer.setEnabled(false);
        final String baseDir = "planner-adapter-service/cloudy-day";
        final String lineItemFileName = "plan-with-empty-schedule.json";

        final URL url = Resources.getResource(String.format("%s/input/%s", baseDir, lineItemFileName));
        final File myFile = new File(url.toURI());
        final String content = FileUtils.readFileToString(myFile, "UTF-8");

        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body(content).statusCode(200).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt()))
                .willReturn(Future.succeededFuture());

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerDataAccessClient, times(1)).updateLineItems(any(), anyInt());
    }

    @Test
    void shouldDropInvalidLineItem() throws Exception {
        tracer.setEnabled(false);
        final String baseDir = "planner-adapter-service/cloudy-day";
        final String lineItemFileName = "invalid-line-items.json";

        final URL url = Resources.getResource(String.format("%s/input/%s", baseDir, lineItemFileName));
        final File myFile = new File(url.toURI());
        final String content = FileUtils.readFileToString(myFile, "UTF-8");

        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body(content).statusCode(200).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt()))
                .willReturn(Future.succeededFuture());

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerDataAccessClient, times(0)).updateLineItems(any(), anyInt());
    }

    @Test
    void shouldNotUpdateSystemStateOnMissingHttpResponseBodyAndStatusCodeNotEqualTo204FromPlannerAdapter() throws Exception {
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().statusCode(200).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET,buildUrl(), "rp", "rp_password", null);
        verify(circuitBreakerSecuredPlannerDataAccessClient, times(0))
                .updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldUpdateSystemStateOnMissingHttpResponseBodyAndStatusCodeEqualTo204FromPlannerAdapter() throws Exception {
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().statusCode(204).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET, buildUrl(), "rp", "rp_password", null);
        verify(circuitBreakerSecuredPlannerDataAccessClient).updateSystemStateWithUTCTime(any());
    }

    private String buildUrl() {
        return plannerAdapterConfiguration.getUrl();
    }

    @Test
    void shouldNotUpdateSystemStateOnEmptyHttpResponseBodyAndStatusCodeNotEqualTo204FromPlannerAdapter() throws Exception {
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body("").statusCode(200).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET, buildUrl(), "rp", "rp_password", null);
        verify(circuitBreakerSecuredPlannerDataAccessClient, times(0))
                .updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldUpdateSystemStateOnBodResponseBodyAndStatusCode200() {
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(
                        HttpResponseContainer.builder().body("{\"bad\": \"body\"").statusCode(200).build()));
        plannerAdapterService.refreshPlans();
        verify(circuitBreakerSecuredPlannerDataAccessClient, times(0)).updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldUpdateSystemStateOnEmptyHttpResponseBodyAndStatusCodeEqualTo204FromPlannerAdapter() throws Exception {
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body("").statusCode(204).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET,buildUrl(), "rp", "rp_password", null);
        verify(circuitBreakerSecuredPlannerDataAccessClient).updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldNotUpdateSystemStateOnErrorStatusCodeFromPlannerAdapter() {
        given(circuitBreakerSecuredPlannerAdapterHttpClient.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body("content").statusCode(400).build()));
        given(circuitBreakerSecuredPlannerDataAccessClient.updateLineItems(any(), anyInt())).willReturn(
                Future.succeededFuture());
        given(circuitBreakerSecuredPlannerDataAccessClient.updateSystemStateWithUTCTime(any())).willReturn(
                Future.succeededFuture(new UpdateResult()));

        plannerAdapterService.refreshPlans();

        verify(circuitBreakerSecuredPlannerAdapterHttpClient)
                .request(HttpMethod.GET, buildUrl(), "rp", "rp_password", null);
        verify(circuitBreakerSecuredPlannerDataAccessClient, times(0))
                .updateSystemStateWithUTCTime(any());
    }

    @Test
    void getPlannerAdapterConfiguration() {
        final PlannerAdapterConfigurations.PlannerAdapterConfiguration plannerAdapterConfigurationLocal
                = plannerAdapterService.getPlannerAdapterConfig();
        assertThat(plannerAdapterConfigurationLocal, samePropertyValuesAs(plannerAdapterConfiguration));
    }

    @Test
    void getPlannerAdapterHttpClient() {
        final CircuitBreakerSecuredPlannerAdapterHttpClient plannerAdapterHttpClient
                = plannerAdapterService.getPlannerAdapterHttpClient();
        assertThat(plannerAdapterHttpClient, samePropertyValuesAs(circuitBreakerSecuredPlannerAdapterHttpClient));
    }

    private String loadTestData(String path) throws Exception {
        File resource1 = new ClassPathResource(path).getFile();
        return new String(Files.readAllBytes(resource1.toPath()));
    }

}

