package org.prebid.pg.gp.server.handler;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.prebid.pg.gp.server.auth.BasicAuthProvider;
import org.prebid.pg.gp.server.auth.BasicAuthUser;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.*;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;
import org.prebid.pg.gp.server.util.Constants;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PlanRequestHandlerTest {

    @Mock
    private Shutdown shutdown;

    @Mock
    private MultiMap params;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerRequest httpRequest;

    @Mock
    private HttpServerResponse httpResponse;

    @Mock
    private CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredPlannerDataAccessClient;

    private PlanRequestHandler planRequestHandler;

    private ObjectMapper objectMapper = new ObjectMapper();

    private ClassLoader classLoader = getClass().getClassLoader();

    private AdminTracer tracer;

    private AlertProxyHttpClient alertHttpClientMock;

    private String vendor = "vendor1";

    @BeforeEach
    void setUp() {
        HostReallocationConfiguration config = new HostReallocationConfiguration();
        int pbsMaxIdlePeriodInSeconds = 180;
        config.setLineItemHasExpiredMin(20);
        objectMapper.findAndRegisterModules();

        tracer = new AdminTracer();
        tracer.setRaw(true);
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        TracerFilters filters = new TracerFilters();
        filters.setVendor(vendor);
        filters.setRegion("us-east");
        tracer.setFilters(filters);

        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        
        planRequestHandler = new PlanRequestHandler(
                circuitBreakerSecuredPlannerDataAccessClient,
                "Service is temporarily unavailable, please try again later", "pbs", true,
                config, pbsMaxIdlePeriodInSeconds, new Metrics(new MetricRegistry()), false, tracer, shutdown,
                alertHttpClientMock, new FakeRandom());

        given(routingContext.request()).willReturn(httpRequest);
        given(routingContext.request().params()).willReturn(params);
        given(routingContext.response()).willReturn(httpResponse);

        lenient().when(httpResponse.setStatusCode(anyInt())).thenReturn(httpResponse);
        lenient().when(httpResponse.putHeader(any(CharSequence.class), any(CharSequence.class))).thenReturn(httpResponse);
    }

    private BasicAuthProvider getBasicAuthProvider(String roles) {
        ServerAuthDataConfiguration serverAuthDataConfiguration = new ServerAuthDataConfiguration();
        serverAuthDataConfiguration.setAuthenticationEnabled(true);

        List<ServerAuthDataConfiguration.Principal> principals = new ArrayList<>();
        ServerAuthDataConfiguration.Principal principal = new ServerAuthDataConfiguration.Principal();
        principal.setUsername("user1");
        principal.setPassword("password1");
        principal.setRoles(roles);
        principals.add(principal);

        serverAuthDataConfiguration.setPrincipals(principals);

        return new BasicAuthProvider(serverAuthDataConfiguration);
    }

    private MultiMap getQueryParams(String baseDir, String queryFileName) throws Exception {
        PbsHost query = objectMapper.readValue(
                new File(classLoader.getResource(String.format("%s/input/%s", baseDir, queryFileName)).getFile()),
                PbsHost.class
        );

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
                .add("region", query.getRegion())
                .add("instanceId", query.getHostInstanceId())
                .add("vendor", query.getVendor());

        return mm;
    }

    @Test
    void shouldRespondWithPlanForOneLineItem() throws Exception {
        String baseDir =  "plan-request-handler/sunny-day-1";

        test(
                baseDir, getQueryParams(baseDir,"query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json", "lineitem-1.json", "response-6.json",
                false, "pbs"
        );
    }

    @Test
    void shouldRespondWithPlanForPartialExpiredSchedules() throws Exception {
        String baseDir =  "plan-request-handler/sunny-day-1";

        test(
                baseDir, getQueryParams(baseDir,"query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json",
                "lineitem-partial-expired-schedule.json", "response-4.json", false, "pbs"
        );
    }

    @Test
    void shouldRespondWithPlanForAllExpiredSchedules() throws Exception {
        String baseDir =  "plan-request-handler/sunny-day-1";

        test(
                baseDir, getQueryParams(baseDir,"query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json",
                "lineitem-all-expired-schedule.json", "response-5.json", false, "pbs"
        );
    }

    @Test
    void shouldReturnPlansForInactiveLineItemsOrEmptyPlans() throws Exception {
        String baseDir =  "plan-request-handler/sunny-day-1";
        
        test(
                baseDir, getQueryParams(baseDir,"query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json", "lineitem-with-empty-plan.json", "response-3.json",
                false, "pbs"
        );
    }

    @Test
    void shouldRespondWithPlanForOneInactiveLineItem() throws Exception {
        String baseDir =  "plan-request-handler/sunny-day-1";

        test(
                baseDir, getQueryParams(baseDir,"query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json", "inactive-lineitem.json",
                "inactive-lineitem-response.json", false, "pbs"
        );
    }

    @Test
    void shouldRespondWith403ForUserWithBadRole() throws Exception {
        String baseDir =  "plan-request-handler/sunny-day-1";

        test(
                baseDir, getQueryParams(baseDir,"query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json", "lineitem-1.json", "response-1.json",
                false, "pbsx"
        );
    }

    @Test
    void shouldRespondWithEmptyPlanListWhenRegionAndInstanceIdAndVendorAreNotFound() throws Exception {
        String baseDir = "plan-request-handler/cloudy";

        test(
                baseDir, getQueryParams(baseDir, "query-1-x.json"),
                null, null, "lineitem-1.json", "response-1.json",
                false, "pbs"
        );
    }

    @Test
    void shouldRespondWithEmptyPlanListWhenVendorAndRegionAndInstanceIdAreNotFoundAndThereAreNoLineItems()
            throws Exception {
        String baseDir = "plan-request-handler/cloudy";

        test(
                baseDir, getQueryParams(baseDir, "query-1-x.json"),
                null, null, null,"response-1.json",
                false, "pbs"
        );
    }


    @Test
    void shouldRespondWithEmptyPlanListWhenVendorAndRegionAndInstanceIdAreFoundButThereAreNoLineItems()
            throws Exception {
        String baseDir = "plan-request-handler/cloudy";

        test(
                baseDir, getQueryParams(baseDir, "query-1.json"),
                "pbs-host-1.json", "reallocated-plan-1.json", null, "response-1.json",
                false, "pbs"
        );
    }

    @Test
    void shouldRespondWithEmptyPlanListWhenVendorAndRegionAndInstanceIdAreFoundButThereAreNoReallocatedPlansAndNoLineItems()
            throws Exception {
        String baseDir = "plan-request-handler/cloudy";

        test(
                baseDir, getQueryParams(baseDir, "query-1-x.json"),
                "pbs-host-1.json", null, null, "response-1.json",
                false, "pbs"
        );
    }

    @Test
    void shouldRespondWithBadRequestErrorIfRegionQueryParamIsMissing() throws Exception {
        String baseDir = "plan-request-handler/bad-query";

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
            .add("instanceId", "fhbp-pbs0000.iad3.fanops.net")
            .add("vendor", vendor);

        test(
                baseDir, mm,
                null, null, null, "response-3.json",
                true, "pbs"
        );
    }

    @Test
    void shouldRespondWithBadRequestErrorIfInstanceIdQueryParamIsMissing() throws Exception {
        String baseDir = "plan-request-handler/bad-query";

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("vendor", vendor);

        test(
                baseDir, mm,
                null, null, null, "response-1.json",
                true, "pbs"
        );
    }

    @Test
    void shouldRespondWithBadRequestErrorIfVendorQueryParamIsMissing() throws Exception {
        String baseDir = "plan-request-handler/bad-query";

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("instanceId", "fhbp-pbs0000.iad3.fanops.net");
        test(
                baseDir, mm,
                null, null, null, "response-5.json",
                true, "pbs"
        );
    }


    @Test
    void shouldRespondWithBadRequestErrorIfAllQueryParamsAreMissing() throws Exception {
        test(
                "plan-request-handler/bad-query",
                null,
                null, null, null, "response-2.json",
                true, "pbs"
        );
    }

    @Test
    void shouldRespondWithBadRequestErrorIfRegionQueryParamIsMissingItsValue() throws Exception {
        String baseDir = "plan-request-handler/bad-query";

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
            .add("region", "")
            .add("vendor", vendor)
            .add("instanceId", "fhbp-pbs0000.iad3.fanops.net");

        test(
                baseDir, mm,
                null, null, null, "response-3.json",
                true, "pbs"
        );
    }

    @Test
    void shouldRespondWithBadRequestErrorIfInstanceIdQueryParamIsMissingItsValue() throws Exception {
        String baseDir = "plan-request-handler/bad-query";

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("vendor", vendor)
                .add("instanceId", "");

        test(
                baseDir, mm,
                null, null, null, "response-4.json",
                true, "pbs"
        );
    }

    @Test
    void shouldRespondWithBadRequestErrorIfVendorQueryParamIsMissingItsValue() throws Exception {
        String baseDir = "plan-request-handler/bad-query";

        MultiMap mm = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("vendor", "")
                .add("instanceId", "fhbp-pbs0000.iad3.fanops.net");

        test(
                baseDir, mm,
                null, null, null, "response-5.json",
                true, "pbs"
        );
    }

    @Test
    void shouldRespondWith500ErrorOnFindActiveHostFailure() throws Exception {
        MultiMap queryParams = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("vendor", vendor)
                .add("instanceId", "fhbp-pbs0000.iad3.fanops.net");

        given(circuitBreakerSecuredPlannerDataAccessClient.findActiveHost(any(), any()))
                    .willReturn(Future.failedFuture("backend exception"));

        given(routingContext.request().params()).willReturn(queryParams);

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        planRequestHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());

        final String expected = "[\"Service is temporarily unavailable, please try again later\"]";

        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }

    @Test
    public void shouldRespondWith500ErrorOnGetReallocatedPlansFailure() throws Exception {
        MultiMap queryParams = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("vendor", vendor)
                .add("instanceId", "fhbp-pbs0000.iad3.fanops.net");

        PbsHost pbsHost = PbsHost.builder().region("us-east").hostInstanceId("fhbp-pbs0000.iad3.fanops.net").build();

        given(circuitBreakerSecuredPlannerDataAccessClient.findActiveHost(any(), any()))
                .willReturn(Future.succeededFuture(pbsHost));

        given(circuitBreakerSecuredPlannerDataAccessClient.getReallocatedPlan(any()))
                .willReturn(Future.failedFuture("backend exception"));

        given(routingContext.request().params()).willReturn(queryParams);

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        planRequestHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());

        final String expected = "[\"Service is temporarily unavailable, please try again later\"]";

        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }

    @Test
    public void shouldRespondWith500ErrorOnGetLineItemsFailure() throws Exception {
        MultiMap queryParams = MultiMap.caseInsensitiveMultiMap()
                .add("region", "us-east")
                .add("vendor", vendor)
                .add("instanceId", "fhbp-pbs0000.iad3.fanops.net");

        PbsHost pbsHost = PbsHost.builder().region("us-east").hostInstanceId("fhbp-pbs0000.iad3.fanops.net").build();

        given(circuitBreakerSecuredPlannerDataAccessClient.findActiveHost(any(), any()))
                .willReturn(Future.succeededFuture(pbsHost));

        ReallocatedPlan rp = ReallocatedPlan.builder().build();
        given(circuitBreakerSecuredPlannerDataAccessClient.getReallocatedPlan(any()))
                .willReturn(Future.succeededFuture(rp));

        given(circuitBreakerSecuredPlannerDataAccessClient.getLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, null))
                .willReturn(Future.failedFuture("backend exception"));

        given(routingContext.request().params()).willReturn(queryParams);

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("pbs"), "user1", "pbs")
        );

        planRequestHandler.handle(routingContext);

        final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpResponse).end(planRequestResponseCaptor.capture());

        verify(httpResponse).setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());

        final String expected = "[\"Service is temporarily unavailable, please try again later\"]";

        assertThat(planRequestResponseCaptor.getValue(), equalTo(expected));
    }

    private void test(
            String baseDir,
            MultiMap queryParams,
            String pbsHostFileName, String reallocatedPlanFileName, String lineItemFileName, String expectedFileName,
            boolean badQueryTest, String roles
    ) throws Exception {

        PbsHost pbsHost = PbsHost.builder().build();
        if (pbsHostFileName != null) {
            pbsHost = objectMapper.readValue(
                    new File(classLoader.getResource(String.format("%s/input/%s", baseDir, pbsHostFileName)).getFile()),
                    PbsHost.class
            );
        }

        List<PbsHost> pbsHosts = new ArrayList<>();
        pbsHosts.add(pbsHost);

        ReallocatedPlan reallocatedPlan = ReallocatedPlan.builder().build();
        if (reallocatedPlanFileName != null) {
            reallocatedPlan = objectMapper.readValue(
                    new File(classLoader.getResource(String.format("%s/input/%s", baseDir, reallocatedPlanFileName)).getFile()),
                    ReallocatedPlan.class
            );
        }

        ArrayList<LineItem> lineItems = new ArrayList<>();
        if (lineItemFileName != null) {
            List<ObjectNode> nodes = objectMapper.readValue(
                    new File(classLoader.getResource(String.format("%s/input/%s", baseDir, lineItemFileName)).getFile()),
                    new TypeReference<ArrayList<ObjectNode>>() {});
            for (ObjectNode node : nodes) {
                lineItems.add(LineItem.from(node, "bidder1", "pg"));
            }
        }

        if (!badQueryTest) {
            lenient().when(circuitBreakerSecuredPlannerDataAccessClient.findActiveHost(any(), any()))
                    .thenReturn(Future.succeededFuture(pbsHost));

            lenient().when(circuitBreakerSecuredPlannerDataAccessClient.findActiveHosts(any()))
                    .thenReturn(Future.succeededFuture(pbsHosts));

            lenient().when(circuitBreakerSecuredPlannerDataAccessClient.getReallocatedPlan(any()))
                    .thenReturn(Future.succeededFuture(reallocatedPlan));

            lenient().when(circuitBreakerSecuredPlannerDataAccessClient
                            .getLineItemsByStatus(any(), any()))
                    .thenReturn(Future.succeededFuture(lineItems));
        }

        lenient().when(routingContext.request().params()).thenReturn(queryParams);

        PlanRequest planRequest = PlanRequest.builder().region("us-east").vendor(vendor).build();
        lenient().when(routingContext.get(PlanRequestHandler.PLAN_REQUEST_KEY)).thenReturn(planRequest);

        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider(roles), "user1", roles)
        );

        planRequestHandler.handle(routingContext);

        if (roles.equals("pbs")) {
            final ArgumentCaptor<String> planRequestResponseCaptor = ArgumentCaptor.forClass(String.class);
            verify(httpResponse).end(planRequestResponseCaptor.capture());

            if (!badQueryTest) {
                verify(httpResponse).setStatusCode(HttpResponseStatus.OK.code());
            } else {
                verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
            }

            final File expectedFile
                    = new File(classLoader.getResource(String.format("%s/output/%s", baseDir, expectedFileName)).getFile());
            final String expectedStr = FileUtils.readFileToString(expectedFile, "UTF-8");

            final JsonNode expected = objectMapper.readTree(expectedStr);
            final JsonNode actual = objectMapper.readTree(planRequestResponseCaptor.getValue());

            assertThat(actual, equalTo(expected));
        } else {
            verify(httpResponse).end();
            verify(httpResponse).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        }
    }

    static class FakeRandom extends Random {
        @Override
        public double nextDouble() {
            return 0.6;
        }
    }
}
