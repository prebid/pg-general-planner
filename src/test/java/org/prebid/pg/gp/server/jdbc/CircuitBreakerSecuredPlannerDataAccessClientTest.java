package org.prebid.pg.gp.server.jdbc;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.PlanRequest;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.model.SystemState;
import org.prebid.pg.gp.server.spring.config.app.CircuitBreakerConfiguration;
import org.prebid.pg.gp.server.util.Constants;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(VertxExtension.class)
public class CircuitBreakerSecuredPlannerDataAccessClientTest {
    private PlannerCircuitBreaker plannerBreaker;
    private PlannerDataAccessClient plannerClientMock = mock(PlannerDataAccessClient.class);
    private CircuitBreakerConfiguration config;
    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient;

    @BeforeEach
    public void setup() {
        config  = new CircuitBreakerConfiguration();
        config.setOpeningThreshold(1);
        config.setClosingIntervalSec(2);
        plannerBreaker = new PlannerCircuitBreaker("foo", Vertx.vertx(), config);
        dataAccessClient = new CircuitBreakerSecuredPlannerDataAccessClient(plannerClientMock, plannerBreaker);
    }

    @Test
    void shouldGetLineItemsByStatus() {
        List<LineItem> lis = Arrays.asList(LineItem.builder().build());
        given(plannerClientMock.getLineItemsByStatus(any(), any())).willReturn(Future.succeededFuture(lis));
        dataAccessClient.getLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, null);
        verify(plannerClientMock).getLineItemsByStatus(any(), any());
    }

    @Test
    void shouldGetCompactLineItemsByStatus() {
        List<LineItem> lis = Arrays.asList(LineItem.builder().build());
        given(plannerClientMock.getCompactLineItemsByStatus(any(), any())).willReturn(Future.succeededFuture(lis));
        dataAccessClient.getCompactLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, Instant.now());
        verify(plannerClientMock).getCompactLineItemsByStatus(any(), any());
    }

    @Test
    void shouldGetLineItemsInactiveSince() {
        List<LineItem> lis = Arrays.asList(LineItem.builder().build());
        given(plannerClientMock.getLineItemsInactiveSince(any())).willReturn(Future.succeededFuture(lis));
        dataAccessClient.getLineItemsInactiveSince(Instant.now().minusSeconds(86400));
        verify(plannerClientMock).getLineItemsInactiveSince(any());
    }

    @Test
    void shouldUpdateLineItems() {
        List<LineItem> lis = Arrays.asList(LineItem.builder().build());
        given(plannerClientMock.updateLineItems(any(), any())).willReturn(Future.succeededFuture());
        dataAccessClient.updateLineItems(lis, 1);
        verify(plannerClientMock).updateLineItems(any(), any());
    }

    @Test
    void shouldFindActiveHost() {
        PbsHost host = PbsHost.builder().build();
        given(plannerClientMock.findActiveHost(any(), any())).willReturn(Future.succeededFuture(host));
        dataAccessClient.findActiveHost(PlanRequest.builder().build(), Instant.now());
        verify(plannerClientMock).findActiveHost(any(), any());
    }

    @Test
    void shouldReadTokenSpendData() {
        List<DeliveryTokenSpendSummary> stats = Arrays.asList(DeliveryTokenSpendSummary.builder().build());
        given(plannerClientMock.readTokenSpendData(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(stats));
        dataAccessClient.readTokenSpendData("host", "region", "vendor", Instant.now());
        verify(plannerClientMock).readTokenSpendData(any(), any(), any(), any());
    }

    @Test
    void shouldGetLineItemsTokenSummary() {
        given(plannerClientMock.getLineItemsTokensSummary(any(), any(), any()))
                .willReturn(Future.succeededFuture(Arrays.asList(LineItemsTokensSummary.builder().build())));
        Instant end = Instant.now();
        dataAccessClient.getLineItemsTokensSummary(end.minus(2, ChronoUnit.HOURS), end, Collections.emptyList());
        verify(plannerClientMock).getLineItemsTokensSummary(any(), any(), any());
    }

    @Test
    void shouldUpdateDeliveryStats() {
        List<DeliveryTokenSpendSummary> stats = Arrays.asList(DeliveryTokenSpendSummary.builder().build());
        given(plannerClientMock.updateDeliveryData(any(), anyInt())).willReturn(Future.succeededFuture());
        dataAccessClient.updateDeliveryStats(stats, 2);
        verify(plannerClientMock).updateDeliveryData(any(),anyInt());
    }

    @Test
    void shouldUpdateAdminEvents() {
        List<AdminEvent> adminEvents = Arrays.asList(AdminEvent.builder().build());
        given(plannerClientMock.updateAdminEvents(any(), anyInt())).willReturn(Future.succeededFuture());
        dataAccessClient.updateAdminEvents(adminEvents, 2);
        verify(plannerClientMock).updateAdminEvents(any(), anyInt());
    }

    @Test
    void shouldFindEarliestActiveAdminEvent() {
        given(plannerClientMock.findEarliestActiveAdminEvent(any(), any(), any()))
                .willReturn(Future.succeededFuture(AdminEvent.builder().build()));
        dataAccessClient.findEarliestActiveAdminEvent("pbs", Registration.builder().build(), Instant.now());
        verify(plannerClientMock).findEarliestActiveAdminEvent(any(), any(), any());
    }

    @Test
    void shouldDeleteAdminEvent() {
        given(plannerClientMock.deleteAdminEvent(any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        dataAccessClient.deleteAdminEvent("uuid1");
        verify(plannerClientMock).deleteAdminEvent(any());
    }

    @Test
    void shouldUpdateRegistration() {
        given(plannerClientMock.updateRegistration(any())).willReturn(Future.succeededFuture(new UpdateResult()));
        dataAccessClient.updateRegistration(Registration.builder().build());
        verify(plannerClientMock).updateRegistration(any());
    }

    @Test
    void shouldFindRegistrations() {
        given(plannerClientMock.findRegistrations(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(Arrays.asList(new HashMap<String, Object>())));
        dataAccessClient.findRegistrations(Instant.now(), "vendor", "region", "host1");
        verify(plannerClientMock).findRegistrations(any(), any(), any(), any());
    }

    @Test
    void shouldGetSystemState() {
        given(plannerClientMock.readUTCTimeValFromSystemState(any())).willReturn(Future.succeededFuture("bar"));
        dataAccessClient.getSystemState("foo");
        verify(plannerClientMock).readUTCTimeValFromSystemState(any());
    }

    @Test
    void shouldUpdateSystemStateWithUTCTime() {
        given(plannerClientMock.updateSystemStateWithUTCTime(any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        dataAccessClient.updateSystemStateWithUTCTime(SystemState.builder().build());
        verify(plannerClientMock).updateSystemStateWithUTCTime(any());
    }

    @Test
    void shouldFindActiveHosts() {
        List<PbsHost> hosts = Arrays.asList(PbsHost.builder().build());
        given(plannerClientMock.findActiveHosts(any())).willReturn(Future.succeededFuture(hosts));
        dataAccessClient.findActiveHosts(Instant.now());
        verify(plannerClientMock).findActiveHosts(any());
    }

    @Test
    void shouldGetReallocatedPlans() {
        given(plannerClientMock.getReallocatedPlan(any(), any(), any()))
                .willReturn(Future.succeededFuture(ReallocatedPlan.builder().build()));
        dataAccessClient.getReallocatedPlan(
                PbsHost.builder().hostInstanceId("foo").vendor("bar").region("bla").build());
        verify(plannerClientMock).getReallocatedPlan(any(), any(), any());
    }

    @Test
    void shouldGetLatestReallocatedPlans() {
        given(plannerClientMock.getLatestReallocatedPlans(any()))
                .willReturn((Future.succeededFuture(Arrays.asList(ReallocatedPlan.builder().build()))));
        dataAccessClient.getLatestReallocatedPlans(Instant.now());
        verify(plannerClientMock).getLatestReallocatedPlans(any());
    }

    @Test
    void shouldUpdateReallocatedPlans() {
        List<ReallocatedPlan> plans = Arrays.asList(ReallocatedPlan.builder().build());
        given(plannerClientMock.updateReallocatedPlans(any(), anyInt())).willReturn(Future.succeededFuture());
        dataAccessClient.updateReallocatedPlans(plans, 2);
        verify(plannerClientMock).updateReallocatedPlans(any(), anyInt());
    }

}
