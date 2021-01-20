package org.prebid.pg.gp.server.handler;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.prebid.pg.gp.server.auth.BasicAuthUser;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.WebConfiguration.CsvMapperFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class LineItemsTokensSummaryHandlerTest extends HandlerTestBase {

    @Mock
    private Shutdown shutdownMock;

    @Mock
    private MultiMap params;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerRequest httpRequest;

    @Mock
    private HttpServerResponse httpResponse;

    @Mock
    private CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClientMock;

    private CsvMapperFactory csvMapperFactory;

    private LineItemsTokensSummaryHandler tokensSummaryHandler;

    private AlertProxyHttpClient alertHttpClientMock;

    @BeforeEach
    void setUp() {
        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        csvMapperFactory = new CsvMapperFactory();

        tokensSummaryHandler = new LineItemsTokensSummaryHandler(
                plannerDataAccessClientMock,
                csvMapperFactory,
                "admin",
                true,
                new Metrics(new MetricRegistry()),
                alertHttpClientMock,
                shutdownMock);

        given(routingContext.response()).willReturn(httpResponse);
    }

    @Test
    void shouldResponseWithOnlyHeaderLineForEmptyRows() {
        String csv = "id,summaryWindowStartTimestamp,summaryWindowEndTimestamp,lineItemId,tokens,"
                + "bidderCode,extLineItemId\n";
        shouldResponseWithHttpStatus200(new ArrayList<>(), new ArrayList<>(), csv, null);
    }

    @Test
    void shouldResponseWithCsvBody() {
        String csv = "id,summaryWindowStartTimestamp,summaryWindowEndTimestamp,lineItemId,tokens,"
                + "bidderCode,extLineItemId\n"
                + "1,2020-03-02T00:00:00.000Z,2020-03-02T01:00:00.000Z,vendor1-l1,10,vendor1,l1\n"
                + "2,2020-03-02T01:00:00.000Z,2020-03-02T02:00:00.000Z,vendor1-l1,10,vendor1,l1\n";
        Instant startTime = Instant.parse("2020-03-02T00:00:00.000Z");
        shouldResponseWithHttpStatus200(buildTokensSummaries(startTime, 1),
                buildTokensSummaries(startTime.plus(1, ChronoUnit.HOURS), 2), csv, null);
    }

    @Test
    void shouldNotProceedWhenShutdownStarted() {
        given(shutdownMock.getInitiating()).willReturn(true);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        tokensSummaryHandler.handle(routingContext);
        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_GATEWAY.code());
        verify(httpResponse).end(anyString());
    }

    @Test
    void shouldResponseWith400ForBadRequest() {
        given(routingContext.request()).willReturn(httpRequest);
        given(routingContext.request().params()).willReturn(params);
        given(params.get("startTime")).willReturn("a,b");
        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("admin", "admin", "admin"), "admin", "admin"));
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);
        tokensSummaryHandler.handle(routingContext);
        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
        verify(httpResponse).end();
    }

    @Test
    void shouldResponseWith400ForBadLineItemIds() {
        given(routingContext.request()).willReturn(httpRequest);
        given(routingContext.request().params()).willReturn(params);
        given(params.get("startTime")).willReturn(Instant.now().minus(2, ChronoUnit.HOURS).toString());
        given(params.get("lineItemIds")).willReturn("badId");
        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("admin", "admin", "admin"), "admin", "admin"));
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);
        tokensSummaryHandler.handle(routingContext);
        verify(httpResponse).setStatusCode(HttpResponseStatus.BAD_REQUEST.code());
        verify(httpResponse).end();
    }

    @Test
    void shouldResponseWith403ForMissingCredential() {
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);
        tokensSummaryHandler.handle(routingContext);
        verify(httpResponse).setStatusCode(HttpResponseStatus.FORBIDDEN.code());
        verify(httpResponse).end();
    }

    private void shouldResponseWithHttpStatus200(List<LineItemsTokensSummary> summaries,
            List<LineItemsTokensSummary> thisHourSummaries, String csv, String metrics) {
        given(routingContext.request()).willReturn(httpRequest);
        given(routingContext.request().params()).willReturn(params);
        given(routingContext.user()).willReturn(
                new BasicAuthUser(getBasicAuthProvider("admin", "admin", "admin"), "admin", "admin"));
        Instant now = Instant.now();
        given(params.get("startTime")).willReturn(now.minus(2, ChronoUnit.HOURS).toString());
        given(params.get("lineItemIds")).willReturn("vendor1-l1");
        given(plannerDataAccessClientMock.getLineItemsTokensSummary(any(), any(), any()))
                .willReturn(Future.succeededFuture(summaries));
        given(plannerDataAccessClientMock.getHourlyLineItemTokens(any(), any(), any()))
                .willReturn(Future.succeededFuture(thisHourSummaries));
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        tokensSummaryHandler.handle(routingContext);

        verify(httpResponse).setStatusCode(HttpResponseStatus.OK.code());
        verify(httpResponse).end(csv);
    }

    private List<LineItemsTokensSummary> buildTokensSummaries(Instant startTime, int id) {
        List<LineItemsTokensSummary> summaries = new ArrayList<>();
        Instant endTime = startTime.plus(1, ChronoUnit.HOURS);
        summaries.add(LineItemsTokensSummary.builder()
                .id(id)
                .lineItemId("vendor1-l1")
                .bidderCode("vendor1")
                .extLineItemId("l1")
                .tokens(10)
                .summaryWindowStartTimestamp(startTime)
                .summaryWindowEndTimestamp(endTime)
                .build());
        return summaries;
    }

}
