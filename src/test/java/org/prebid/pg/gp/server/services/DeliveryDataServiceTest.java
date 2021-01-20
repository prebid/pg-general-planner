package org.prebid.pg.gp.server.services;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredDeliveryDataHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.TracerFilters;
import org.prebid.pg.gp.server.spring.config.app.DeliveryDataConfiguration;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
class DeliveryDataServiceTest {

    private static Vertx vertx;

    private final String hostName = "MyMac";

    private final String tsStr = "2019-07-18T18:54:46Z";

    private String vendor = "vendor1";

    private String region = "us-east-1";

    private String url;

    private AdminTracer tracer;

    private DeliveryDataService deliveryDataService;

    private DeliveryDataConfiguration deliveryDataConfiguration;

    private CircuitBreakerSecuredDeliveryDataHttpClient httpClientMock;

    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock;

    private AlertProxyHttpClient alertHttpClientMock;

    private Shutdown shutdown;

    private StatsCache statsCache = new StatsCache();

    @BeforeAll
    static void prepare() throws Exception {
        vertx = Vertx.vertx();
    }

    @BeforeEach
    void setUpBeforeEach() throws Exception {
        tracer = new AdminTracer();
        TracerFilters filters = new TracerFilters();
        filters.setVendor(vendor);
        filters.setRegion("us-east-1");
        filters.setBidderCode(vendor.toUpperCase());
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        tracer.setFilters(filters);
        deliveryDataConfiguration = new DeliveryDataConfiguration();
        deliveryDataConfiguration.setEnabled(true);
        deliveryDataConfiguration.setUsername("rp");
        deliveryDataConfiguration.setPassword("rp_password");
        deliveryDataConfiguration.setUrl("some.com");
        deliveryDataConfiguration.setInitialDelaySec(1);
        deliveryDataConfiguration.setRefreshPeriodSec(1);
        deliveryDataConfiguration.setStartTimeInPastSec(60);
        int pbsMaxIdlePeriodInSeconds = 300;
        url = String.format("%s?vendor=%s&region=%s", deliveryDataConfiguration.getUrl(), vendor, region);

        shutdown = new Shutdown();

        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        httpClientMock = mock(CircuitBreakerSecuredDeliveryDataHttpClient.class);
        dataAccessClientMock = mock(CircuitBreakerSecuredPlannerDataAccessClient.class);
        deliveryDataService = new DeliveryDataService(vertx,
                deliveryDataConfiguration,
                pbsMaxIdlePeriodInSeconds,
                dataAccessClientMock,
                httpClientMock,
                new Metrics(new MetricRegistry()),
                tracer,
                shutdown,
                alertHttpClientMock,
                statsCache);
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        vertx.close();
    }

    @Test
    void testInitializeWhileRefreshDisabled() {
        deliveryDataConfiguration.setEnabled(false);
        deliveryDataService.initialize();
        verify(httpClientMock, times(0)).request(any(), any(), any(), any(), any());
    }

    @Test
    void shouldRefreshDeliveryData() throws Exception {
        final String baseDir = "delivery-data-service/sunny-day";
        final String statsFileName = "stats-1.json";
        final File statsFile = new File(
                Resources.getResource(String.format("%s/input/%s", baseDir, statsFileName)).toURI());
        final String content = FileUtils.readFileToString(statsFile, "UTF-8");

        given(httpClientMock.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body(content).statusCode(200).build()));
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(activePbsHosts()));

        deliveryDataService.refreshDeliveryData("");
        verify(httpClientMock)
                .request(HttpMethod.GET, url, "rp", "rp_password", "");
    }

    @Test
    void shouldRefreshWhenShutdownStarted() {
        shutdown.setInitiating(true);
        deliveryDataService.refreshDeliveryData(null);
        verify(httpClientMock, times(0)).request(any(), any(), any(), any(), any());
    }

    @Test
    void shouldNotUpdateSystemStateOnMissingHttpResponseBodyAndStatusCodeNotEqualTo204FromStatsService() {
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(activePbsHosts()));
        given(httpClientMock.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().statusCode(200).build()));

        deliveryDataService.refreshDeliveryData("");
        verify(httpClientMock).request(HttpMethod.GET, url, "rp", "rp_password", "");
        assertThat(statsCache.get().isEmpty(), is(true));
    }

    @Test
    void shouldUpdateSystemStateOnMissingHttpResponseBodyAndStatusCodeEqualTo204FromStatsService() {
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(activePbsHosts()));
        given(httpClientMock.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().statusCode(204).build()));

        deliveryDataService.refreshDeliveryData("");
        verify(httpClientMock).request(HttpMethod.GET, url, "rp", "rp_password", "");
        assertThat(statsCache.get().isEmpty(), is(true));
    }

    @Test
    void shouldNotUpdateSystemStateOnEmptyHttpResponseBodyAndStatusCodeNotEqualTo204FromStatsService() {
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(activePbsHosts()));
        given(httpClientMock.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body("").statusCode(200).build()));

        deliveryDataService.refreshDeliveryData("");
        verify(httpClientMock).request(HttpMethod.GET, url, "rp", "rp_password", "");
        assertThat(statsCache.get().isEmpty(), is(true));
    }

    @Test
    void shouldUpdateSystemStateOnEmptyHttpResponseBodyAndStatusCodeEqualTo204FromStatsService() {
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(activePbsHosts()));
        given(httpClientMock.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body("").statusCode(204).build()));

        deliveryDataService.refreshDeliveryData("");
        verify(httpClientMock).request(HttpMethod.GET, url, "rp", "rp_password", "");
        assertThat(statsCache.get().isEmpty(), is(true));
    }

    @Test
    void shouldNotUpdateSystemStateOnErrorStatusCodeFromPlannerAdapter() throws Exception {
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(activePbsHosts()));
        given(httpClientMock.request(any(), any(), any(), any(), any())).willReturn(
                Future.succeededFuture(HttpResponseContainer.builder().body("content").statusCode(400).build()));

        deliveryDataService.refreshDeliveryData("");
        verify(httpClientMock).request(HttpMethod.GET, url, "rp", "rp_password", "");
        assertThat(statsCache.get().isEmpty(), is(true));
    }

    private List<PbsHost> activePbsHosts() {
        return Arrays.asList(PbsHost.builder()
                .vendor(vendor)
                .region(region)
                .build());
    }

    private String url() {
        return deliveryDataConfiguration.getUrl() + "?vendor=vendor1&region=us-east-1";
    }

}