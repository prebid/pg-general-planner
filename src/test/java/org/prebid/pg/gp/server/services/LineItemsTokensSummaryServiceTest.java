package org.prebid.pg.gp.server.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.LineItemsHistoryClient;
import org.prebid.pg.gp.server.jdbc.LineItemsTokensSummaryClient;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.SystemState;
import org.prebid.pg.gp.server.spring.config.app.TokensSummaryConfiguration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(VertxExtension.class)
public class LineItemsTokensSummaryServiceTest {

    private static  Vertx vertx;

    private AlertProxyHttpClient alertHttpClientMock;

    private LineItemsHistoryClient lineItemHistoryClientMock;

    private LineItemsTokensSummaryClient tokensSummaryClientMock;

    private TokensSummaryConfiguration tokensSummaryConfig = new TokensSummaryConfiguration();

    private Shutdown shutdown = new Shutdown();

    private LineItemsTokensSummaryService summaryService;

    private final String lineItemHistorySummarySystemStateTag = "li_history_summary_ts";

    @BeforeAll
    static void prepare() throws Exception {
        vertx = Vertx.vertx();
    }

    @BeforeEach
    void setUpBeforeEach() throws Exception {
        tokensSummaryConfig.setGranularSummaryMinute(3);
        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        tokensSummaryClientMock = mock(LineItemsTokensSummaryClient.class);
        lineItemHistoryClientMock = mock(LineItemsHistoryClient.class);
        summaryService = new LineItemsTokensSummaryService(vertx, lineItemHistoryClientMock,
                tokensSummaryClientMock, alertHttpClientMock, tokensSummaryConfig, shutdown);
    }

    @Test
    void shouldSummarizeProperly() {
        Instant endTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant startTime = endTime.minus(1, ChronoUnit.HOURS);
        SystemState systemState = SystemState.builder()
                .tag(lineItemHistorySummarySystemStateTag)
                .val(endTime.toString())
                .build();
        List<LineItemsTokensSummary> summaries = new ArrayList<>();
        summaries.add(LineItemsTokensSummary.builder()
                .lineItemId("vendor1-l1")
                .bidderCode("vendor1")
                .extLineItemId("l1")
                .tokens(20)
                .summaryWindowStartTimestamp(startTime)
                .summaryWindowEndTimestamp(endTime)
                .build());
        ArgumentCaptor<List<LineItemsTokensSummary>> listCaptor = ArgumentCaptor.forClass(List.class);
        given(lineItemHistoryClientMock.readUTCTimeValFromSystemState(any()))
                .willReturn(Future.succeededFuture(startTime.toString()));
        given(lineItemHistoryClientMock.findLineItemTokens(any(), any()))
                .willReturn(Future.succeededFuture(summaries));
        given(tokensSummaryClientMock.saveLineItemsTokenSummary(listCaptor.capture()))
                .willReturn(Future.succeededFuture());
        given(lineItemHistoryClientMock.updateSystemStateWithUTCTime(any()))
                .willReturn(Future.succeededFuture());

        Future<Void> result = summaryService.summarize();

        assertThat(result.succeeded(), equalTo(true));
        verify(lineItemHistoryClientMock).readUTCTimeValFromSystemState(lineItemHistorySummarySystemStateTag);
        verify(lineItemHistoryClientMock).findLineItemTokens(
                startTime, startTime.plus(tokensSummaryConfig.getGranularSummaryMinute(), ChronoUnit.MINUTES));
        List<LineItemsTokensSummary> rs = listCaptor.getValue();
        verify(lineItemHistoryClientMock).updateSystemStateWithUTCTime(systemState);
        assertThat(rs.size(), equalTo(1));
        int tokens = 20 * 60 / tokensSummaryConfig.getGranularSummaryMinute();
        assertThat(rs.get(0).getTokens(), equalTo(tokens));
    }

    @Test
    void shouldNotSummarizeEmptyInterval() {
        Instant endTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant startTime = endTime;
        given(lineItemHistoryClientMock.readUTCTimeValFromSystemState(any()))
                .willReturn(Future.succeededFuture(startTime.toString()));

        Future<Void> result = summaryService.summarize();

        assertThat(result.succeeded(), equalTo(true));
        verify(lineItemHistoryClientMock).readUTCTimeValFromSystemState(lineItemHistorySummarySystemStateTag);
        verify(lineItemHistoryClientMock, never()).findLineItemTokens(startTime, endTime);
    }
}
