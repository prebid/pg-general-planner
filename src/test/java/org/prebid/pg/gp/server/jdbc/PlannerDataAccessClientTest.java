package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.*;
import org.prebid.pg.gp.server.spring.config.app.LineItemsTokensSummaryConfiguration;
import org.prebid.pg.gp.server.util.Constants;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PlannerDataAccessClientTest {

    private LineItemsClient lineItemsClientMock;

    private TokenSpendClient tokenSpendClientMock;

    private SystemStateClient systemStateClientMock;

    private RegistrationClient registrationClientMock;

    private ReallocatedPlansClient reallocatedPlansClientMock;

    private LineItemsTokensSummaryClient lineItemsTokensSummaryClientMock;

    private AdminEventClient adminEventClientMock;

    private LineItemsHistoryClient lineItemsHistoryClientMock;

    private AlertProxyHttpClient alertHttpClientMock;

    private LineItemsTokensSummaryConfiguration tokenSummaryConfigurationMock;

    private JDBCClient jdbcClientStub;

    private PlannerDataAccessClient dataAccessClient;

    private String vendor = "vendor1";

    @BeforeEach
    void init() {
        lineItemsClientMock = mock(LineItemsClient.class);
        tokenSpendClientMock = mock(TokenSpendClient.class);
        systemStateClientMock = mock(SystemStateClient.class);
        registrationClientMock = mock(RegistrationClient.class);
        reallocatedPlansClientMock = mock(ReallocatedPlansClient.class);
        lineItemsTokensSummaryClientMock = mock(LineItemsTokensSummaryClient.class);
        lineItemsHistoryClientMock = mock(LineItemsHistoryClient.class);
        adminEventClientMock = mock(AdminEventClient.class);
        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        tokenSummaryConfigurationMock = mock(LineItemsTokensSummaryConfiguration.class);
        jdbcClientStub = new JDBCClientStub();
        dataAccessClient = new PlannerDataAccessClient(
                jdbcClientStub,
                lineItemsClientMock,
                tokenSpendClientMock,
                systemStateClientMock,
                registrationClientMock,
                reallocatedPlansClientMock,
                lineItemsTokensSummaryClientMock,
                lineItemsHistoryClientMock,
                adminEventClientMock,
                new Metrics(new MetricRegistry()),
                alertHttpClientMock,
                tokenSummaryConfigurationMock
        );
    }

    @Test
    void shouldUpdateSystemState() {
        UpdateResult updateResult = new UpdateResult();
        given(systemStateClientMock.updateSystemStateWithUTCTime(any(), any()))
                .willReturn(Future.succeededFuture(updateResult));

        dataAccessClient.updateSystemStateWithUTCTime(SystemState.builder().build());
        verify(systemStateClientMock).updateSystemStateWithUTCTime(any(), any());
    }

    @Test
    void shouldReadUTCTimeValFromSystemState() {
        given(systemStateClientMock.readUTCTimeValFromSystemState(any(), any()))
                .willReturn(Future.succeededFuture("ok"));
        dataAccessClient.readUTCTimeValFromSystemState("foo");
        verify(systemStateClientMock).readUTCTimeValFromSystemState(any(), any());
    }

    @Test
    void shouldUpdateRegistration() {
        given(registrationClientMock.updateRegistration(any(), any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        dataAccessClient.updateRegistration(Registration.builder().build());
        verify(registrationClientMock).updateRegistration(any(), any());
    }

    @Test
    void shouldFindRegistrations() {
        List<Map<String, Object>> list = new ArrayList<>();
        given(registrationClientMock.findRegistrations(any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(list));
        dataAccessClient.findRegistrations(Instant.now(), vendor, "east", "host1");
        verify(registrationClientMock).findRegistrations(any(), any(), any(), any(), any());
    }

    @Test
    void shouldUpdateAdminEvents() {
        Future<UpdateResult> future = Future.succeededFuture(new UpdateResult());
        given(adminEventClientMock.updateAdminEvents(any(), any())).willReturn(future);
        AdminEvent cmd = AdminEvent.builder().build();
        List<AdminEvent> events = Arrays.asList(cmd, cmd, cmd, cmd);
        dataAccessClient.updateAdminEvents(events, 2);
        verify(adminEventClientMock, times(2)).updateAdminEvents(any(), any());
    }

    @Test
    void shouldFindEarliestActiveAdminEvent() {
        given(adminEventClientMock.findEarliestActiveAdminEvent(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(AdminEvent.builder().build()));
        dataAccessClient.findEarliestActiveAdminEvent("pbs", Registration.builder().build(), Instant.now());
        verify(adminEventClientMock).findEarliestActiveAdminEvent(any(), any(), any(), any());
    }

    @Test
    void shouldDeleteAdminEvent() {
        given(adminEventClientMock.deleteAdminEvent(any(), any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        dataAccessClient.deleteAdminEvent("uuid1");
        verify(adminEventClientMock).deleteAdminEvent(any(), any());
    }

    @Test
    void shouldUpdateDeliveryData() {
        DeliveryTokenSpendSummary stats = DeliveryTokenSpendSummary.builder().build();
        List<DeliveryTokenSpendSummary> statsList = Arrays.asList(stats, stats, stats, stats);
        given(tokenSpendClientMock.updateTokenSpendData(any(), any()))
                .willReturn(Future.succeededFuture(new UpdateResult()));
        dataAccessClient.updateDeliveryData(statsList, 2);
        verify(tokenSpendClientMock, times(2)).updateTokenSpendData(any(), any());
    }

    @Test
    void shouldReadTokenSpendData() {
        List<DeliveryTokenSpendSummary> stats = Collections.singletonList(DeliveryTokenSpendSummary.builder().build());
        given(tokenSpendClientMock.getTokenSpendData(any(), anyString(), anyString(), anyString(), any()))
                .willReturn(Future.succeededFuture(stats));
        dataAccessClient.readTokenSpendData("host", "east", vendor, Instant.now());
        verify(tokenSpendClientMock).getTokenSpendData(any(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void shouldUpdateReallocatedPlans() {
        Future<List<Integer>> future = Future.succeededFuture(Arrays.asList(1, 2));
        given(reallocatedPlansClientMock.updateReallocatedPlans(any(), any())).willReturn(future);
        ReallocatedPlan plan = ReallocatedPlan.builder().build();
        List<ReallocatedPlan> plans = Arrays.asList(plan, plan, plan, plan);
        dataAccessClient.updateReallocatedPlans(plans, 2);
        verify(reallocatedPlansClientMock, times(2)).updateReallocatedPlans(any(), any());
    }

    @Test
    void shouldGetLineItemsTokensSummary() {
        List<LineItemsTokensSummary> summaries = Arrays.asList(LineItemsTokensSummary.builder().build(),
                LineItemsTokensSummary.builder().build());
        given(tokenSummaryConfigurationMock.getPageSize()).willReturn(2);
        given(lineItemsTokensSummaryClientMock.getLineItemsTokensSummaryCount(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(4));
        given(lineItemsTokensSummaryClientMock.getLineItemsTokensSummary(any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(summaries));
        Instant now = Instant.now();
        dataAccessClient.getLineItemsTokensSummary(now.minus(1, ChronoUnit.HOURS), now, new ArrayList<>());

        verify(lineItemsTokensSummaryClientMock, times(2))
                .getLineItemsTokensSummary(any(), any(), any(), any(), any());
    }

    @Test
    void shouldFindActiveHosts() {
        List<PbsHost> pbsHosts = Collections.singletonList(PbsHost.builder().build());
        given(registrationClientMock.findActiveHosts(any(), any())).willReturn(Future.succeededFuture(pbsHosts));
        dataAccessClient.findActiveHosts(Instant.now());
        verify(registrationClientMock).findActiveHosts(any(), any());
    }

    @Test
    void shouldFindActiveHost() {
        PbsHost pbsHost = PbsHost.builder().build();
        given(registrationClientMock.findActiveHost(any(), any(), any())).willReturn(Future.succeededFuture(pbsHost));
        dataAccessClient.findActiveHost(pbsHost, Instant.now());
        verify(registrationClientMock).findActiveHost(any(), any(), any());
    }

    @Test
    void shouldGetReallocatedPlans() {
        given(reallocatedPlansClientMock.getReallocatedPlan(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(ReallocatedPlan.builder().build()));
        dataAccessClient.getReallocatedPlan("host", "region", "vendor");
        verify(reallocatedPlansClientMock).getReallocatedPlan(any(), any(), any(), any());
    }

    @Test
    void shouldGetLatestReallocatedPlans() {
        given(reallocatedPlansClientMock.getLatestReallocatedPlans(any(), any()))
                .willReturn(Future.succeededFuture(Arrays.asList(ReallocatedPlan.builder().build())));
        dataAccessClient.getLatestReallocatedPlans(Instant.now());
        verify(reallocatedPlansClientMock).getLatestReallocatedPlans(any(), any());
    }

    @Test
    void shouldGetLineItemsByStatus() {
        List<LineItem> lineItems = Collections.singletonList(LineItem.builder().build());
        given(lineItemsClientMock.getLineItemsByStatus(any(), any(), any())).willReturn(Future.succeededFuture(lineItems));
        dataAccessClient.getLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, null);
        verify(lineItemsClientMock).getLineItemsByStatus(any(), any(), any());
    }

    @Test
    void shouldGetCompactLineItemsByStatus() {
        List<LineItem> lineItems = Collections.singletonList(LineItem.builder().build());
        given(lineItemsClientMock.getCompactLineItemsByStatus(any(), any(), any())).willReturn(Future.succeededFuture(lineItems));
        dataAccessClient.getCompactLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, Instant.now());
        verify(lineItemsClientMock).getCompactLineItemsByStatus(any(), any(), any());
    }

    @Test
    void shouldGetLineItemsInactiveSince() {
        List<LineItem> lineItems = Collections.singletonList(LineItem.builder().build());
        given(lineItemsClientMock.getLineItemsInactiveSince(any(), any()))
                .willReturn(Future.succeededFuture(lineItems));
        dataAccessClient.getLineItemsInactiveSince(Instant.now().minusSeconds(86400));
        verify(lineItemsClientMock).getLineItemsInactiveSince(any(), any());
    }

    @Test
    void shouldUpdateLineItems() {
        LineItem li = LineItem.builder().build();
        List<LineItem> lis = Arrays.asList(li, li, li, li);
        given(lineItemsClientMock.updateLineItems(any(), any()))
                .willReturn(Future.succeededFuture(new  UpdateResult()));
        dataAccessClient.updateLineItems(lis, 2);
        verify(lineItemsClientMock, times(2)).updateLineItems(any(), any());
    }

    @Test
    public void shuldBuildBatches() {
        List<Integer> entries = new ArrayList<>();
        List<List<Integer>> batches = PlannerDataAccessClient.buildBatches(entries, 2);
        assertThat(batches.size(), equalTo(0));

        for (int i = 0; i < 10; i++) {
            entries.add(i);
        }

        batches = PlannerDataAccessClient.buildBatches(entries, 2);
        assertThat(batches.size(), equalTo(5));

        batches = PlannerDataAccessClient.buildBatches(entries, 3);
        assertThat(batches.size(), equalTo(4));
        assertThat(batches.get(3).size(), equalTo(1));
    }

    public static class JDBCClientStub implements JDBCClient {
        @Override
        public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
            handler.handle(Future.succeededFuture(mock(SQLConnection.class)));
            return null;
        }

        @Override
        public void close(Handler<AsyncResult<Void>> handler) {
            // do nothing
        }

        @Override
        public void close() {
           // do nothing
        }
    }

}
