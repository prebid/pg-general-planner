package org.prebid.pg.gp.server.jdbc;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.javatuples.Pair;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.LineItemIdentity;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.PageRequest;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.model.SystemState;
import org.prebid.pg.gp.server.spring.config.app.LineItemsTokensSummaryConfiguration;
import org.prebid.pg.gp.server.util.Constants;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A facade for {@code JDBC} layer to access database.
 */
public class PlannerDataAccessClient {

    private static final Logger logger = LoggerFactory.getLogger(PlannerDataAccessClient.class);

    private final Metrics metrics;

    private final JDBCClient jdbcClient;

    private final LineItemsClient lineItemsClient;

    private final TokenSpendClient tokenSpendClient;

    private final SystemStateClient systemStateClient;

    private final RegistrationClient registrationClient;

    private final ReallocatedPlansClient reallocatedPlansClient;

    private final LineItemsTokensSummaryClient lineItemsTokensSummaryClient;

    private final LineItemsHistoryClient lineItemsHistoryClient;

    private final AlertProxyHttpClient alertHttpClient;

    private final AdminEventClient adminEventClient;

    private final LineItemsTokensSummaryConfiguration lineItemsTokensSummaryConfiguration;

    public PlannerDataAccessClient(
            JDBCClient jdbcClient,
            LineItemsClient lineItemsClient,
            TokenSpendClient tokenSpendClient,
            SystemStateClient systemStateClient,
            RegistrationClient registrationClient,
            ReallocatedPlansClient reallocatedPlansClient,
            LineItemsTokensSummaryClient lineItemsTokensSummaryClient,
            LineItemsHistoryClient lineItemsHistoryClient,
            AdminEventClient adminEventClient,
            Metrics metrics,
            AlertProxyHttpClient alertHttpClient,
            LineItemsTokensSummaryConfiguration lineItemsTokensSummaryConfiguration
    ) {

        this.jdbcClient = Objects.requireNonNull(jdbcClient);
        this.systemStateClient = Objects.requireNonNull(systemStateClient);
        this.registrationClient = Objects.requireNonNull(registrationClient);
        this.lineItemsClient = Objects.requireNonNull(lineItemsClient);
        this.tokenSpendClient = Objects.requireNonNull(tokenSpendClient);
        this.reallocatedPlansClient = Objects.requireNonNull(reallocatedPlansClient);
        this.lineItemsTokensSummaryClient = Objects.requireNonNull(lineItemsTokensSummaryClient);
        this.lineItemsHistoryClient = Objects.requireNonNull(lineItemsHistoryClient);
        this.adminEventClient = Objects.requireNonNull(adminEventClient);
        this.metrics = Objects.requireNonNull(metrics);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
        this.lineItemsTokensSummaryConfiguration = Objects.requireNonNull(lineItemsTokensSummaryConfiguration);
    }

    public Future<SQLConnection> connect() {
        final Future<SQLConnection> future = Future.future();
        jdbcClient.getConnection(future);
        return future.recover(this::logConnectionError);
    }

    private Future<SQLConnection> logConnectionError(Throwable ex) {
        String msg = String.format(
                "Exception::Cannot connect to database::%s. Cause::%s", ex.getMessage(), ex.getCause());
        logger.error(msg);
        alertHttpClient.raiseEvent(Constants.GP_PLANNER_DB_CLIENT_ERROR, AlertPriority.HIGH, msg);
        return Future.failedFuture(ex);
    }

    Future<UpdateResult> updateSystemStateWithUTCTime(SystemState systemState) {
        return connect()
                .compose(sqlConnection -> systemStateClient.updateSystemStateWithUTCTime(sqlConnection, systemState));
    }

    Future<String> readUTCTimeValFromSystemState(String tag) {
        return connect()
                .compose(sqlConnection -> systemStateClient.readUTCTimeValFromSystemState(sqlConnection, tag));
    }

    Future<UpdateResult> updateRegistration(Registration registration) {
        return connect()
                .compose(sqlConnection -> registrationClient.updateRegistration(sqlConnection, registration));
    }

    Future<List<Map<String, Object>>> findRegistrations(
            Instant activeSince, String vendor, String region, String instanceId) {
        return connect()
                .compose(sqlConnection ->
                        registrationClient.findRegistrations(sqlConnection, activeSince, vendor, region, instanceId));
    }

    Future<Void> updateDeliveryData(List<DeliveryTokenSpendSummary> tokenSpendSummaries, int batchSize) {
        return updateInBatch(tokenSpendSummaries, batchSize, this::updateDeliveryData, "update-token-data-all");
    }

    private Future<UpdateResult> updateDeliveryData(List<DeliveryTokenSpendSummary> tokenSpendSummaries) {
        return connect()
                .compose(sqlConnection -> tokenSpendClient.updateTokenSpendData(sqlConnection, tokenSpendSummaries));
    }

    Future<List<LineItemsTokensSummary>> getHourlyLineItemTokens(
            Instant startTime, Instant endTime, List<LineItemIdentity> lineItemIds) {
        List<Pair<Instant, Instant>> intervals = getIntervals(startTime, endTime);
        if (intervals.isEmpty()) {
            return Future.succeededFuture(Collections.emptyList());
        }
        List<LineItemsTokensSummary> list = new ArrayList<>();
        final Pair<Instant, Instant> firstInterval = intervals.get(0);
        Future<List<LineItemsTokensSummary>> result =
                findLineItemTokens(firstInterval.getValue0(), firstInterval.getValue1(), lineItemIds);
        for (int i = 1; i < intervals.size(); i++) {
            Pair<Instant, Instant> interval = intervals.get(i);
            result = result.compose(summaries -> {
                list.addAll(summaries);
                return findLineItemTokens(interval.getValue0(), interval.getValue1(), lineItemIds);
            });
        }
        return result.compose(summaries -> {
            list.addAll(summaries);
            return Future.succeededFuture(list);
        });
    }

    private List<Pair<Instant, Instant>> getIntervals(Instant startTime, Instant finalEndTime) {
        List<Pair<Instant, Instant>> intervals = new ArrayList<>();
        while (startTime.isBefore(finalEndTime)) {
            Instant endTime = startTime.plus(1, ChronoUnit.HOURS);
            if (endTime.isAfter(finalEndTime)) {
                intervals.add(Pair.with(startTime, finalEndTime));
                break;
            }
            intervals.add(Pair.with(startTime, endTime));
            startTime = endTime;
        }
        return intervals;
    }

    private Future<List<LineItemsTokensSummary>> findLineItemTokens(
            Instant updatedAtOrAfter, Instant updatedBefore, List<LineItemIdentity> lineItemIds) {
        return connect()
                .compose(sqlConnection -> lineItemsHistoryClient.findLineItemTokens(
                        sqlConnection, updatedAtOrAfter, updatedBefore, lineItemIds));
    }

    Future<List<LineItemsTokensSummary>> getLineItemsTokensSummary(Instant startTime, Instant endTime,
            List<String> lineItemIds) {
        final int size = lineItemsTokensSummaryConfiguration.getPageSize();
        final PageRequest page = PageRequest.builder()
                .number(0)
                .size(size)
                .build();
        final List<LineItemsTokensSummary> tokenSummaries = new ArrayList<>();
        return connect()
                .compose(sqlConnection -> lineItemsTokensSummaryClient.getLineItemsTokensSummaryCount(
                        sqlConnection, startTime, endTime, lineItemIds))
                .compose(count -> {
                    final int pages = (count + size - 1) / size;
                    Future<List<LineItemsTokensSummary>> result =
                            getLineItemsTokensSummary(startTime, endTime, lineItemIds, page);
                    for (int i = 1; i < pages; i++) {
                        result = result.compose(list -> {
                            tokenSummaries.addAll(list);
                            page.setNumber(page.getNumber() + 1);
                            return getLineItemsTokensSummary(startTime, endTime, lineItemIds, page);
                        });
                    }
                    return result.compose(list -> {
                        tokenSummaries.addAll(list);
                        return Future.succeededFuture(tokenSummaries);
                    });
                });
    }

    private Future<List<LineItemsTokensSummary>> getLineItemsTokensSummary(Instant startTime, Instant endTime,
            List<String> lineItemIds, PageRequest page) {
        return connect()
                .compose(sqlConnection -> lineItemsTokensSummaryClient.getLineItemsTokensSummary(
                        sqlConnection, startTime, endTime, lineItemIds, page));
    }

    Future<List<DeliveryTokenSpendSummary>> readTokenSpendData(
            String hostInstanceId, String region, String vendor, Instant updatedSince) {
        return connect()
                .compose(sqlConnection -> tokenSpendClient.getTokenSpendData(
                        sqlConnection, hostInstanceId, region, vendor, updatedSince));
    }

    Future<Void> updateReallocatedPlans(List<ReallocatedPlan> reallocatedPlans, Integer batchSize) {
        if (reallocatedPlans.isEmpty()) {
            return Future.succeededFuture();
        }
        final List<List<ReallocatedPlan>> batches = buildBatches(reallocatedPlans, batchSize);
        final Future<List<Integer>> init = updateReallocatedPlans(batches.get(0));
        final List<Function<List<Integer>, Future<List<Integer>>>> handlers = new ArrayList<>();
        for (int i = 1; i < batches.size(); i++) {
            final int index = i;
            Function<List<Integer>, Future<List<Integer>>> handler =
                    asyncResult -> updateReallocatedPlans(batches.get(index));
            handlers.add(handler);
        }
        final long start = System.currentTimeMillis();
        final Future<List<Integer>> finalFuture = chain(init, handlers);
        return finalFuture.map(rs -> {
            logger.info("All updateReallocatedPlans method processed in {0}", System.currentTimeMillis() - start);
            metrics.updateTimer(metricName("update-reallocated-plans"), System.currentTimeMillis() - start);
            return null;
        });
    }

    private Future<List<Integer>> updateReallocatedPlans(List<ReallocatedPlan> reallocatedPlans) {
        return connect()
                .compose(sqlConnection ->
                        reallocatedPlansClient.updateReallocatedPlans(sqlConnection, reallocatedPlans));
    }

    Future<List<PbsHost>> findActiveHosts(Instant activeSince) {
        return connect()
                .compose(sqlConnection -> registrationClient.findActiveHosts(sqlConnection, activeSince));
    }

    Future<PbsHost> findActiveHost(PbsHost pbsHost, Instant activeSince) {
        return connect()
                .compose(sqlConnection -> registrationClient.findActiveHost(sqlConnection, pbsHost, activeSince));
    }

    Future<ReallocatedPlan> getReallocatedPlan(String hostInstanceId, String region, String vendor) {
        return connect()
                .compose(sqlConnection -> reallocatedPlansClient.getReallocatedPlan(
                                sqlConnection, hostInstanceId, region, vendor));
    }

    Future<List<ReallocatedPlan>> getLatestReallocatedPlans(Instant updatedSince) {
        return connect().compose(
                sqlConnection -> reallocatedPlansClient.getLatestReallocatedPlans(sqlConnection, updatedSince));
    }

    Future<List<LineItem>> getLineItemsByStatus(String status, Instant endTime) {
        return connect()
                .compose(connection -> lineItemsClient.getLineItemsByStatus(connection, status, endTime));
    }

    Future<List<LineItem>> getCompactLineItemsByStatus(String status, Instant endTime) {
        return connect()
                .compose(connection -> lineItemsClient.getCompactLineItemsByStatus(connection, status, endTime));
    }

    Future<List<LineItem>> getLineItemsInactiveSince(Instant timestamp) {
        return connect()
                .compose(connection -> lineItemsClient.getLineItemsInactiveSince(connection, timestamp));
    }

    Future<Void> updateAdminEvents(List<AdminEvent> entities, int batchSize) {
        return updateInBatch(entities, batchSize, this::updateAdminEvents, "update-admin-command-batches");
    }

    private Future<UpdateResult> updateAdminEvents(List<AdminEvent> entities) {
        return connect().compose(sqlConnection -> {
            try {
                return adminEventClient.updateAdminEvents(sqlConnection, entities);
            } catch (Exception ex) {
                sqlConnection.close();
                return Future.failedFuture(ex);
            }
        });
    }

    Future<AdminEvent> findEarliestActiveAdminEvent(String app, Registration registration, Instant expiryAt) {
        return connect().compose(sqlConnection -> {
            try {
                return adminEventClient.findEarliestActiveAdminEvent(sqlConnection, app, registration, expiryAt);
            } catch (Exception ex) {
                sqlConnection.close();
                return Future.failedFuture(ex);
            }
        });
    }

    Future<UpdateResult> deleteAdminEvent(String id) {
        return connect().compose(sqlConnection -> {
            try {
                return adminEventClient.deleteAdminEvent(sqlConnection, id);
            } catch (Exception ex) {
                sqlConnection.close();
                return Future.failedFuture(ex);
            }
        });
    }

    Future<Void> updateLineItems(List<LineItem> lineItems, Integer batchSize) {
        return updateInBatch(lineItems, batchSize, this::updateLineItems, "update-line-items-all-batches");
    }

    private Future<UpdateResult> updateLineItems(List<LineItem> lineItemBatch) {
        return connect()
                .compose(sqlConnection -> lineItemsClient.updateLineItems(sqlConnection, lineItemBatch));
    }

    private <T, R> Future<Void> updateInBatch(List<T> items, Integer batchSize, UpdateHandler<T, R> updateHandler,
            String method) {

        if (items.isEmpty()) {
            return Future.succeededFuture();
        }

        final List<List<T>> batches = buildBatches(items, batchSize);
        logger.info("{0}::Number of batches to process::{1}", method, batches.size());
        final Future<R> init = updateHandler.update(batches.get(0));
        final List<Function<R, Future<R>>> handlers = new ArrayList<>();
        for (int i = 1; i < batches.size(); i++) {
            final int index = i;
            handlers.add(asyncResult -> updateHandler.update(batches.get(index)));
        }
        final long start = System.currentTimeMillis();
        final Future<?> finalFuture = chain(init, handlers);
        return finalFuture.map(rs -> {
            logger.info("{0}::updateInBatch::processed in {1}ms", method, System.currentTimeMillis() - start);
            metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
            return null;
        });
    }

    @FunctionalInterface
    interface UpdateHandler<T, R> {
        Future<R> update(List<T> items);
    }

    static <T> List<List<T>> buildBatches(List<T> entries, int batchSize) {
        int size = entries.size();
        List<List<T>> batches = new ArrayList<>();
        int start = 0;
        while (start < size) {
            int end = start + batchSize;
            end = Math.min(end, size);
            batches.add(entries.subList(start, end));
            start = end;
        }
        return batches;
    }

    private <T> Future<T> chain(Future<T> init, List<Function<T, Future<T>>> handlers) {
        Future<T> result = init;
        for (Function<T, Future<T>> handler : handlers) {
            result = result.compose(handler);
        }
        return result;
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }
}

