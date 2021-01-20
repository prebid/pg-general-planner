package org.prebid.pg.gp.server.jdbc;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.exception.GeneralPlannerException;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.DeliverySchedule;
import org.prebid.pg.gp.server.model.LineItemIdentity;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.SystemState;
import org.prebid.pg.gp.server.model.DeliverySchedule.Plan;
import org.prebid.pg.gp.server.model.DeliverySchedule.Token;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A client to access database for line item history information.
 */
public class LineItemsHistoryClient {

    private static final Logger logger = LoggerFactory.getLogger(LineItemsHistoryClient.class);

    private static final String GET_UNORDERED_LINE_ITEMS_HISTORY_SQL =
            "SELECT bidder_code, line_item_id, line_item, updated_at "
            + "FROM line_items_history "
            + "WHERE updated_at >= ? AND updated_at < ? ";

    private static final String GET_ORDERED_LINE_ITEMS_HISTORY_SQL =
            GET_UNORDERED_LINE_ITEMS_HISTORY_SQL
            + "ORDER BY updated_at ASC";

    private final Metrics metrics;

    private final JDBCClient jdbcClient;

    private final SystemStateClient systemStateClient;

    private final AlertProxyHttpClient alertHttpClient;

    public LineItemsHistoryClient(
            JDBCClient jdbcClient,
            Metrics metrics,
            SystemStateClient systemStateClient,
            AlertProxyHttpClient alertHttpClient
    ) {
        this.metrics = Objects.requireNonNull(metrics);
        this.jdbcClient = Objects.requireNonNull(jdbcClient);
        this.systemStateClient = Objects.requireNonNull(systemStateClient);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
    }

    public Future<List<LineItemsTokensSummary>> findLineItemTokens(Instant updatedAtOrAfter, Instant updatedBefore) {
        return connect()
                .compose(sqlConnection ->
                        findLineItemTokens(sqlConnection, updatedAtOrAfter, updatedBefore, Collections.emptyList()));
    }

    public Future<List<LineItemsTokensSummary>> findLineItemTokens(SQLConnection sqlConnection,
            Instant updatedAtOrAfter, Instant updatedBefore, List<LineItemIdentity> lineItemIds) {
        final Future<ResultSet> resultSetFuture = Future.future();
        final long start = System.currentTimeMillis();
        final String method = "read-line-items-history";

        logger.info("Get line items updated at or after {0} and before {1}", updatedAtOrAfter, updatedBefore);

        sqlConnection.queryWithParams(buildQuerySql(lineItemIds),
                new JsonArray().add(updatedAtOrAfter).add(updatedBefore),
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logAppError(
                                method,
                                String.format("Failure in summarizing line items for %s - %s",
                                        updatedAtOrAfter, updatedBefore),
                                ar.cause()
                        );
                    }
                    resultSetFuture.handle(ar);
                });

        return resultSetFuture.map(rs -> mapToLineItemsTokensSummary(rs, updatedAtOrAfter, updatedBefore));
    }

    String buildQuerySql(List<LineItemIdentity> lineItemIds) {
        if (CollectionUtils.isEmpty(lineItemIds)) {
            return GET_ORDERED_LINE_ITEMS_HISTORY_SQL;
        }
        StringBuilder sb = new StringBuilder(GET_UNORDERED_LINE_ITEMS_HISTORY_SQL);
        sb.append(" AND (");
        for (int i = 0; i < lineItemIds.size(); i++) {
            LineItemIdentity li = lineItemIds.get(i);
            if (i > 0) {
                sb.append(" OR ");
            }
            sb.append("(bidder_code = '").append(li.getBidderCode()).append("'")
                    .append(" AND ")
                    .append("line_item_id = '").append(li.getLineItemId()).append("')");
        }
        sb.append(") ORDER BY updated_at ASC");
        String query = sb.toString();
        logger.debug("query sql:{0}", query);
        return query;
    }

    private List<LineItemsTokensSummary> mapToLineItemsTokensSummary(
            ResultSet resultSet, Instant updatedAtOrAfter, Instant updatedBefore) {

        final Instant createdAt = Instant.now();
        final Map<String, Map<Instant, Integer>> liStartTimeTokensMap = new HashMap<>();
        final Map<String, LineItemsTokensSummary> liTokensSummaryMap = new HashMap<>();

        if (resultSet.getResults() != null) {
            for (final JsonArray ja : resultSet.getResults()) {
                try {
                    String li = String.format("%s-%s", ja.getString(0), ja.getString(1));
                    DeliverySchedule schedules = Json.decodeValue(ja.getString(2), DeliverySchedule.class);
                    for (Plan plan : schedules.getDeliverySchedules()) {
                        Instant startTimestamp = Instant.parse(plan.getStartTimestamp());
                        if (!startTimestamp.isBefore(updatedBefore) || updatedAtOrAfter.isAfter(startTimestamp)) {
                            continue;
                        }
                        for (Token token : plan.getTokens()) {
                            if (token.getClazz() != 1) {
                                continue;
                            }
                            Map<Instant, Integer> intervalSummaries =
                                    liStartTimeTokensMap.computeIfAbsent(li, key -> new HashMap<>());
                            liTokensSummaryMap.computeIfAbsent(li, key ->
                                    LineItemsTokensSummary.builder()
                                            .bidderCode(ja.getString(0))
                                            .extLineItemId(ja.getString(1))
                                            .lineItemId(li)
                                            .createdAt(createdAt)
                                            .summaryWindowStartTimestamp(updatedAtOrAfter)
                                            .summaryWindowEndTimestamp(updatedBefore)
                                            .tokens(0)  // default
                                            .build());

                            if (!intervalSummaries.containsKey(startTimestamp)) {
                                intervalSummaries.put(startTimestamp, token.getTotal());
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Exception in creating line item top tokens::{0}", e.getCause().getMessage());
                    throw new GeneralPlannerException(e);
                }
            }
        }

        for (Map.Entry<String, LineItemsTokensSummary> entry : liTokensSummaryMap.entrySet()) {
            int totalTokens = liStartTimeTokensMap.get(entry.getKey()).values()
                    .stream()
                    .mapToInt(num -> num)
                    .sum();
            entry.getValue().setTokens(totalTokens);
        }

        return new ArrayList<>(liTokensSummaryMap.values());
    }

    public Future<UpdateResult> updateSystemStateWithUTCTime(SystemState systemState) {
        return connect()
                .compose(sqlConnection -> systemStateClient.updateSystemStateWithUTCTime(sqlConnection, systemState));
    }

    public Future<String> readUTCTimeValFromSystemState(String tag) {
        return connect()
                .compose(sqlConnection -> systemStateClient.readUTCTimeValFromSystemState(sqlConnection, tag));
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }

    private void logAppError(String method, String msg, Throwable cause) {
        msg = String.format("%s::%s", msg, cause.getMessage());
        logger.error(msg);
        alertHttpClient.raiseEvent(Constants.GP_PLANNER_DB_CLIENT_ERROR, AlertPriority.MEDIUM, msg);
        metrics.incCounter(metricName(method + ".exc"));
    }

    public Future<SQLConnection> connect() {
        final Future<SQLConnection> future = Future.future();
        jdbcClient.getConnection(future);
        return future.recover(this::logConnectionError);
    }

    private Future<SQLConnection> logConnectionError(Throwable ex) {
        String msg = String.format(
                "Exception::Cannot connect to database::%s. Cause::%s", ex.getMessage(), ex.getCause()
            );
        logger.error(msg);
        alertHttpClient.raiseEvent(Constants.GP_PLANNER_DB_CLIENT_ERROR, AlertPriority.HIGH, msg);
        return Future.failedFuture(ex);
    }
}
