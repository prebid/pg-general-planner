package org.prebid.pg.gp.server.jdbc;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.ResultSetType;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.PageRequest;
import org.prebid.pg.gp.server.util.Constants;
import org.prebid.pg.gp.server.util.StringUtil;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A client to access database for statistics information of line item tokens.
 */
public class LineItemsTokensSummaryClient {

    private static final Logger logger = LoggerFactory.getLogger(LineItemsTokensSummaryClient.class);

    private static final String INSERT_SQL =
            "INSERT INTO line_items_tokens_summary ("
            + "summary_window_start_timestamp, summary_window_end_timestamp, "
            + "line_item_id, ext_line_item_id, bidder_code, tokens, created_at) "
            + "VALUES  ";

    private static final String GET_BY_START_END_TIMESTAMP_SQL =
            "SELECT line_item_id, bidder_code, ext_line_item_id, tokens, "
            + "summary_window_start_timestamp, summary_window_end_timestamp, id "
            + "FROM line_items_tokens_summary "
            + "WHERE summary_window_start_timestamp >= ? AND summary_window_end_timestamp < ? ";

    private static final String GET_BY_START_END_TIMESTAMP_PAGEABLE_SQL =
            GET_BY_START_END_TIMESTAMP_SQL + "LIMIT ? OFFSET ?";

    private static final String GET_COUNT_BY_START_END_TIMESTAMP_SQL =
            "SELECT count(*) "
            + "FROM line_items_tokens_summary "
            + "WHERE summary_window_start_timestamp >= ? AND summary_window_end_timestamp < ? ";

    private static final String GET_BY_START_END_TIMESTAMP_AND_LINE_ITEM_ID_SQL =
            GET_BY_START_END_TIMESTAMP_SQL + "AND line_item_id IN ";

    private static final String GET_COUNT_BY_START_END_TIMESTAMP_AND_LINE_ITEM_ID_SQL =
            GET_COUNT_BY_START_END_TIMESTAMP_SQL + "AND line_item_id IN ";

    private JDBCClient jdbcClient;

    private Metrics metrics;

    private AlertProxyHttpClient alertHttpClient;

    public LineItemsTokensSummaryClient(JDBCClient jdbcClient, Metrics metrics, AlertProxyHttpClient alertHttpClient) {
        this.jdbcClient = jdbcClient;
        this.metrics = metrics;
        this.alertHttpClient = alertHttpClient;
    }

    Future<Integer> getLineItemsTokensSummaryCount(SQLConnection connection,
            Instant startTime, Instant endTime, List<String> lineItemIds) {
        if (startTime == null || endTime == null || !startTime.isBefore(endTime)) {
            connection.close();
            return Future.succeededFuture(0);
        }
        final Future<ResultSet> future = Future.future();
        JsonArray params = new JsonArray()
                .add(startTime)
                .add(endTime);
        final long start = System.currentTimeMillis();
        final String method = "read-line-items-tokens-summary-count";
        connection.setOptions(new SQLOptions().setResultSetType(ResultSetType.FORWARD_ONLY))
                .queryWithParams(buildCountQuerySql(lineItemIds), params, ar -> {
                    connection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logger.error("Failure reading line_items_tokens_summary_count for {0}::{1}::{2} => {3}",
                                startTime, endTime, lineItemIds, ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    future.handle(ar);
                });
        return future.map(this::mapToLineItemsTokensSummaryCount);
    }

    Future<List<LineItemsTokensSummary>> getLineItemsTokensSummary(SQLConnection connection,
            Instant startTime, Instant endTime, List<String> lineItemIds, PageRequest page) {
        if (startTime == null || endTime == null || !startTime.isBefore(endTime)) {
            connection.close();
            return Future.succeededFuture(new ArrayList<>());
        }

        final Future<ResultSet> future = Future.future();
        JsonArray params = new JsonArray()
                .add(startTime)
                .add(endTime)
                .add(page.getSize())
                .add(page.getNumber() * page.getSize());
        final long start = System.currentTimeMillis();
        final String method = "read-line-items-tokens-summary";
        connection.setOptions(new SQLOptions().setResultSetType(ResultSetType.FORWARD_ONLY))
                .queryWithParams(buildQuerySql(lineItemIds), params, ar -> {
                    connection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logger.error("Failure in reading line_items_tokens_summary for {0}::{1}::{2} => {3}",
                                startTime, endTime, lineItemIds, ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    future.handle(ar);
                });
        return future.map(this::mapToLineItemsTokensSummary);
    }

    private String buildQuerySql(List<String> lineItemIds) {
        if (lineItemIds.isEmpty()) {
            return GET_BY_START_END_TIMESTAMP_PAGEABLE_SQL;
        }
        StringBuilder sb = new StringBuilder(GET_BY_START_END_TIMESTAMP_AND_LINE_ITEM_ID_SQL)
                .append("('")
                .append(String.join("','", lineItemIds))
                .append("') ")
                .append("LIMIT ? OFFSET ? ");

        return sb.toString();
    }

    private String buildCountQuerySql(List<String> lineItemIds) {
        if (lineItemIds.isEmpty()) {
            return GET_COUNT_BY_START_END_TIMESTAMP_SQL;
        }
        StringBuilder sb = new StringBuilder(GET_COUNT_BY_START_END_TIMESTAMP_AND_LINE_ITEM_ID_SQL);
        sb.append("('");
        sb.append(String.join("','", lineItemIds));
        sb.append("')");
        return sb.toString();
    }

    private List<LineItemsTokensSummary> mapToLineItemsTokensSummary(ResultSet resultSet) {
        List<LineItemsTokensSummary> tokensSummaries = new ArrayList<>();
        if (resultSet.getResults() != null) {
            for (JsonArray row : resultSet.getResults()) {
                LineItemsTokensSummary tokenSummary = LineItemsTokensSummary.builder()
                        .lineItemId(row.getString(0))
                        .bidderCode(row.getString(1))
                        .extLineItemId(row.getString(2))
                        .tokens(row.getInteger(3))
                        .summaryWindowStartTimestamp(row.getInstant(4))
                        .summaryWindowEndTimestamp(row.getInstant(5))
                        .id(row.getInteger(6))
                        .build();
                tokensSummaries.add(tokenSummary);
            }
        }
        return tokensSummaries;
    }

    private int mapToLineItemsTokensSummaryCount(ResultSet resultSet) {
        int count = 0;
        if (resultSet.getResults() != null && resultSet.getResults().size() == 1) {
            count = resultSet.getResults().get(0).getInteger(0);
        }
        return count;
    }

    public Future<UpdateResult> saveLineItemsTokenSummary(List<LineItemsTokensSummary> tokensSummaries) {
        return connect()
                .compose(sqlConnection -> saveLineItemsTokenSummary(sqlConnection, tokensSummaries));
    }

    private Future<UpdateResult> saveLineItemsTokenSummary(SQLConnection connection,
            List<LineItemsTokensSummary> tokensSummaries) {
        if (tokensSummaries.isEmpty()) {
            connection.close();
            return Future.succeededFuture();
        }
        final Future<UpdateResult> future = Future.future();
        StringBuilder sb = StringUtil.appendRepeatedly(
                new StringBuilder(INSERT_SQL), "(?, ?, ?, ?, ?, ?, ?)", ",", tokensSummaries.size());
        final String method = "save-line-items-tokens-summary";

        logger.info("Save line items summaries");
        connection.updateWithParams(sb.toString(), fillInLineItemsTokensSummaryParams(tokensSummaries),
                ar -> {
                    connection.close();
                    if (ar.succeeded()) {
                        logger.info("Save line items summary succeeded.");
                    } else {
                        logAppError(method, "Failure in saving line items tokens summary", ar.cause());
                    }
                    future.handle(ar);
                });
        return future;
    }

    private JsonArray fillInLineItemsTokensSummaryParams(List<LineItemsTokensSummary> tokensSummaries) {
        JsonArray params = new JsonArray();
        for (LineItemsTokensSummary summary : tokensSummaries) {
            params.add(summary.getSummaryWindowStartTimestamp())
                .add(summary.getSummaryWindowEndTimestamp())
                .add(summary.getLineItemId())
                .add(summary.getExtLineItemId())
                .add(summary.getBidderCode())
                .add(summary.getTokens())
                .add(summary.getCreatedAt());
        }
        return params;
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
        return Future.failedFuture(ex);
    }

    private void logAppError(String method, String msg, Throwable cause) {
        msg = String.format("%s::%s", msg, cause.getMessage());
        logger.error(msg);
        alertHttpClient.raiseEvent(Constants.GP_PLANNER_TOKENS_SUMMARY_CLIENT_ERROR, AlertPriority.MEDIUM, msg);
        metrics.incCounter(metricName(method + ".exc"));
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }

}
