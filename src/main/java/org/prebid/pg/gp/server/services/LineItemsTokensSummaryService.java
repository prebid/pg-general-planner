package org.prebid.pg.gp.server.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.javatuples.Pair;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.LineItemsHistoryClient;
import org.prebid.pg.gp.server.jdbc.LineItemsTokensSummaryClient;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.SystemState;
import org.prebid.pg.gp.server.spring.config.app.TokensSummaryConfiguration;
import org.prebid.pg.gp.server.util.Constants;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A service to do the line item tokens statistics calculation periodically.
 */
public class LineItemsTokensSummaryService {

    private static final Logger logger = LoggerFactory.getLogger(LineItemsTokensSummaryService.class);

    private static final Integer INTERVAL_MINUTE = 60;

    private static final String LINE_ITEM_HISTORY_SUMMARY_TS = "li_history_summary_ts";

    private final Vertx vertx;

    private final AlertProxyHttpClient alertHttpClient;

    private final LineItemsHistoryClient lineItemHistoryClient;

    private final LineItemsTokensSummaryClient tokensSummaryClient;

    private final TokensSummaryConfiguration tokensSummaryConfiguration;

    private final Shutdown shutdown;

    public LineItemsTokensSummaryService(
            Vertx vertx,
            LineItemsHistoryClient lineItemHistoryDataAccessClient,
            LineItemsTokensSummaryClient tokensSummaryClient,
            AlertProxyHttpClient alertHttpClient,
            TokensSummaryConfiguration tokensSummaryConfiguration,
            Shutdown shutdown
    ) {
        this.vertx = Objects.requireNonNull(vertx);
        this.lineItemHistoryClient = Objects.requireNonNull(lineItemHistoryDataAccessClient);
        this.tokensSummaryClient = Objects.requireNonNull(tokensSummaryClient);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
        this.tokensSummaryConfiguration = Objects.requireNonNull(tokensSummaryConfiguration);
        this.shutdown = Objects.requireNonNull(shutdown);
    }

    /**
     * Set up timer to do the line item tokens statistics calculation periodically.
     */
    public void initialize() {
        if (!tokensSummaryConfiguration.getEnabled()) {
            logger.info("Token Summary Service is disabled");
            return;
        }
        vertx.setTimer(tokensSummaryConfiguration.getInitialDelayMinute() * 60000L, id -> schedule());
    }

    private void schedule() {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            logger.info("Token Summary Service::Server shutdown has been initiated");
            return;
        }
        logger.info("Tokens Summary Service started");
        summarize().setHandler(ar -> {
            if (ar.succeeded()) {
                logger.info("Tokens Summary Service completed successfully");
            } else {
                logger.info("Tokens Summary Service failed");
            }
            Instant now = Instant.now();
            Instant nextRun = now.truncatedTo(ChronoUnit.HOURS)
                    .plus(tokensSummaryConfiguration.getRunOnMinute(), ChronoUnit.MINUTES);
            if (!nextRun.isAfter(now)) {
                nextRun = nextRun.plus(1, ChronoUnit.HOURS);
            }
            long duration = Duration.between(now, nextRun).toMinutes();
            logger.info("now::{0}|nextRun::{1}", now, nextRun);
            if (duration < 1) {
                // startup without enough delay
                duration = INTERVAL_MINUTE;
            }
            vertx.setTimer(duration * 60000, id -> schedule());
        });
    }

    public Future<Void> summarize() {
        Future<Void> future = Future.future();
        lineItemHistoryClient.readUTCTimeValFromSystemState(LINE_ITEM_HISTORY_SUMMARY_TS)
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        summarizeSinceLastRun(ar, future);
                    } else {
                        future.fail(ar.cause());
                    }
                });
        return future;
    }

    private void summarizeSinceLastRun(AsyncResult<String> asyncResult, Future<Void> future) {
        logger.info("LastEndTimestamp::{0}", asyncResult.result());
        final Instant finalEndTime = currentHour();
        Instant startTime = getStartTime(asyncResult.result());
        List<Pair<Instant, Instant>> intervals = getIntervals(startTime, finalEndTime);
        if (intervals.isEmpty()) {
            logger.info("Tokens Summary Service completed with empty intervals");
            future.complete();
        } else {
            final List<Function<Void, Future<Void>>> handlers = new ArrayList<>();
            for (int i = 1; i < intervals.size(); i++) {
                final int index = i;
                handlers.add(async -> summarizeForInterval(intervals.get(index)));
            }
            final Future<Void> init = summarizeForInterval(intervals.get(0));
            chain(init, handlers).setHandler(future::handle);
        }
    }

    private Future<List<LineItemsTokensSummary>> summarizeForGranularInterval(Instant startTime, Instant endTime) {
        Future<List<LineItemsTokensSummary>> future = Future.future();
        int count = INTERVAL_MINUTE / tokensSummaryConfiguration.getGranularSummaryMinute();
        if (count == 0) {
            return Future.succeededFuture(Collections.emptyList());
        }

        final Instant createdAt = Instant.now();
        Instant start = startTime;
        Instant end;
        List<Pair<Instant, Instant>> intervals = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            end = start.plus(tokensSummaryConfiguration.getGranularSummaryMinute(), ChronoUnit.MINUTES);
            intervals.add(Pair.with(start, end));
            start = end;
        }
        if (INTERVAL_MINUTE % tokensSummaryConfiguration.getGranularSummaryMinute() > 0) {
            intervals.add(Pair.with(start, endTime));
        }

        Future<List<LineItemsTokensSummary>> result = Future.succeededFuture(new ArrayList<>());
        Map<String, LineItemsTokensSummary> liSummaries = new HashMap<>();
        for (int i = 0; i < intervals.size(); i++) {
            Pair<Instant, Instant> interval = intervals.get(i);
            result = result.compose(rs -> {
                accumulateGranularSummary(rs, liSummaries, startTime, endTime, createdAt);
                return lineItemHistoryClient.findLineItemTokens(interval.getValue0(), interval.getValue1());
            });
        }
        result.setHandler(async -> {
            if (async.succeeded()) {
                accumulateGranularSummary(async.result(), liSummaries, startTime, endTime, createdAt);
                future.complete(new ArrayList(liSummaries.values()));
            } else {
                future.fail(async.cause());
            }
        });
        return future;
    }

    private void accumulateGranularSummary(List<LineItemsTokensSummary> granularSummaries,
            Map<String, LineItemsTokensSummary> liSummaries, Instant start, Instant end, Instant createdAt) {
        for (LineItemsTokensSummary granularSummary : granularSummaries) {
            LineItemsTokensSummary summary = null;
            String lineItemId = granularSummary.getLineItemId();
            if (!liSummaries.containsKey(lineItemId)) {
                summary = LineItemsTokensSummary.builder()
                        .bidderCode(granularSummary.getBidderCode())
                        .extLineItemId(granularSummary.getExtLineItemId())
                        .lineItemId(granularSummary.getLineItemId())
                        .createdAt(createdAt)
                        .summaryWindowStartTimestamp(start)
                        .summaryWindowEndTimestamp(end)
                        .tokens(granularSummary.getTokens())
                        .build();
                liSummaries.put(lineItemId, summary);
            } else {
                summary = liSummaries.get(lineItemId);
                summary.setTokens(summary.getTokens() + granularSummary.getTokens());
            }
        }
    }

    private Future<Void> summarizeForInterval(Pair<Instant, Instant> pair) {
        Future<Void> future = Future.future();
        final Instant startTime = pair.getValue0();
        final Instant endTime = pair.getValue1();
        String msg = String.format("Tokens Summary for interval: %s - %s", startTime, endTime);
        logger.info(msg);
        summarizeForGranularInterval(startTime, endTime)
                .compose(tokensSummaryClient::saveLineItemsTokenSummary)
                .compose(rs -> {
                    SystemState systemState = SystemState.builder()
                            .tag(LINE_ITEM_HISTORY_SUMMARY_TS)
                            .val(endTime.toString())
                            .build();
                    return lineItemHistoryClient.updateSystemStateWithUTCTime(systemState);
                })
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        future.complete();
                    } else {
                        String error = "Error while " + msg;
                        alertHttpClient.raiseEvent(Constants.GP_PLANNER_LINE_ITEMS_TOKENS_SUMMARY_SERVICE_ERROR,
                                AlertPriority.HIGH, error);
                        future.fail(ar.cause());
                    }
                });
        return future;
    }

    private List<Pair<Instant, Instant>> getIntervals(Instant startTime, Instant finalEndTime) {
        List<Pair<Instant, Instant>> intervals = new ArrayList<>();
        while (startTime.isBefore(finalEndTime)) {
            Instant endTime = startTime.plus(1, ChronoUnit.HOURS);
            if (endTime.isAfter(finalEndTime)) {
                break;
            }
            intervals.add(Pair.with(startTime, endTime));
            startTime = endTime;
        }
        return intervals;
    }

    private Instant getStartTime(String lastEndTimestamp) {
        final int diff = 1;
        Instant startTimestamp = Instant.parse(lastEndTimestamp);
        return startTimestamp.equals(Instant.EPOCH)
                ? currentHour().minus(diff, ChronoUnit.HOURS)
                : startTimestamp;
    }

    private Instant currentHour() {
        return Instant.now().truncatedTo(ChronoUnit.HOURS);
    }

    private <T> Future<T> chain(Future<T> init, List<Function<T, Future<T>>> handlers) {
        Future<T> result = init;
        for (Function<T, Future<T>> handler : handlers) {
            result = result.compose(handler);
        }
        return result;
    }

}
