package org.prebid.pg.gp.server.services;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.*;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(VertxExtension.class)
class HostReallocationServiceTest {

    private static Vertx vertx = Vertx.vertx();

    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock;

    private HostBasedTokenReallocation reallocationAlgoMock;

    private HostReallocationConfiguration configMock;

    private HostReallocationService service;

    private StatsCache statsCacheMock;

    private Shutdown shutdown = new Shutdown();

    private String vendor = "vendor1";

    @BeforeEach
    void setUpBeforeEach() {
        dataAccessClientMock = mock(CircuitBreakerSecuredPlannerDataAccessClient.class);
        reallocationAlgoMock = mock(HostBasedTokenReallocation.class);
        configMock = mock(HostReallocationConfiguration.class);
        statsCacheMock = mock(StatsCache.class);
        int pbsMaxIdlePeriodInSeconds = 60;

        service = new HostReallocationService(
                vertx, configMock, pbsMaxIdlePeriodInSeconds, dataAccessClientMock, reallocationAlgoMock,
                new Metrics(new MetricRegistry()), shutdown, statsCacheMock);
    }

    @Test
    void shouldCalculate() {
        commonSetup();
        given(dataAccessClientMock.getReallocatedPlan(any()))
                .willReturn(Future.succeededFuture(ReallocatedPlan.builder().build()));
    
        service.calculate(null);

        commonVerify();
        verify(dataAccessClientMock).updateReallocatedPlans(any(), anyInt());
    }

    @Test
    void shouldCalculateWithEndTime() {
        commonSetup();
        given(reallocationAlgoMock.calculate(any(), any(), any(), any()))
                .willReturn(Arrays.asList(ReallocatedPlan.builder().build()));

        Instant endTime = Instant.now();
        service.calculate(endTime);

        commonVerify();
        verify(dataAccessClientMock).updateReallocatedPlans(any(), anyInt());
    }

    @Test
    void shouldCalculateFailed() {
        commonSetup();
        given(dataAccessClientMock.getLatestReallocatedPlans(any()))
                .willReturn(Future.failedFuture(new RuntimeException("error")));

        service.calculate(null);

        verify(dataAccessClientMock, times(0)).updateReallocatedPlans(any(), anyInt());
        verify(reallocationAlgoMock, times(0)).calculate(any(), any(), any(), any());
    }

    @Test
    void shouldNotCalculateIfShutdownStarted() {
        shutdown.setInitiating(true);
        service.calculate(null);
        verify(dataAccessClientMock, times(0)).getCompactLineItemsByStatus(any(), any());
    }

    @Test
    void shouldCalculateIfNoActiveLineItems() {
        given(dataAccessClientMock.getCompactLineItemsByStatus(any(), any()))
                .willReturn(Future.succeededFuture(Collections.emptyList()));
        service.calculate(null);
        verify(dataAccessClientMock, times(0)).findActiveHosts(any());
    }

    @Test
    void shouldCalculateIfNoActiveHosts() {
        List<LineItem> lineItems = Arrays.asList(LineItem.builder().build());
        given(dataAccessClientMock.getCompactLineItemsByStatus(any(), any()))
                .willReturn(Future.succeededFuture(lineItems));
        given(dataAccessClientMock.findActiveHosts(any()))
                .willReturn(Future.succeededFuture(Collections.emptyList()));
        service.calculate(null);
        verify(statsCacheMock, times(0)).get();
    }

    @Test
    void shouldCalculateProperlyWhenPlanIsMissing() {
        commonSetup();
        given(dataAccessClientMock.getLatestReallocatedPlans(any()))
                .willReturn(Future.succeededFuture(Collections.emptyList()));

        service.calculate(null);

        commonVerify();
        verify(reallocationAlgoMock, times(2)).calculate(any(), any(), any(), any());
        verify(dataAccessClientMock).getLatestReallocatedPlans(any());
        verify(dataAccessClientMock).updateReallocatedPlans(any(), anyInt());
    }

    private void commonSetup() {
        List<LineItem> lineItems = new ArrayList<>();
        LineItem lineItem1 = LineItem.builder()
                .lineItemId("1")
                .bidderCode("pgvendor1").build();
        lineItems.add(lineItem1);
        List<PbsHost> hosts = new ArrayList<>();
        PbsHost host = PbsHost.builder().vendor(vendor).region("east").hostInstanceId("11").build();
        hosts.add(host);
        List<DeliveryTokenSpendSummary> stats1 = new ArrayList<>();
        DeliveryTokenSpendSummary summary1 = DeliveryTokenSpendSummary.builder()
                .vendor(vendor).region("east").instanceId("11").lineItemId("pgvendor1-1").extLineItemId("1")
                .build();
        stats1.add(summary1);
        given(dataAccessClientMock.getCompactLineItemsByStatus(any(), any()))
                .willReturn(Future.succeededFuture(lineItems));
        given(dataAccessClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(hosts));
        given(statsCacheMock.get()).willReturn(stats1);
        given(dataAccessClientMock.getLatestReallocatedPlans(any()))
                .willReturn(Future.succeededFuture(Collections.emptyList()));

        given(reallocationAlgoMock.calculate(any(), any(), any(), any())).willReturn(Collections.emptyList());
        given(configMock.getDbStoreBatchSize()).willReturn(2);
        given(dataAccessClientMock.updateReallocatedPlans(any(), anyInt())).willReturn(Future.succeededFuture());
    }

    private void commonVerify() {
        verify(dataAccessClientMock).getCompactLineItemsByStatus(any(), any());
        verify(dataAccessClientMock).findActiveHosts(any());
        verify(dataAccessClientMock).getLatestReallocatedPlans(any());
    }

}

