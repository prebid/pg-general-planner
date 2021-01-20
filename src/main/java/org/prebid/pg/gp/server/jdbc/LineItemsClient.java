package org.prebid.pg.gp.server.jdbc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.exception.GeneralPlannerException;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.util.Constants;
import org.prebid.pg.gp.server.util.JsonUtil;
import org.prebid.pg.gp.server.util.StringUtil;
import org.prebid.pg.gp.server.util.Validators;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A client to access database for line item information.
 */
public class LineItemsClient {

    private static final Logger logger = LoggerFactory.getLogger(LineItemsClient.class);

    private static final String SELECT_COMMON_FIELDS_SQL =
            "SELECT line_item, bidder_code, line_item_id FROM line_items ";

    private static final String GET_LINE_ITEMS_BY_STATUS_SQL =
            SELECT_COMMON_FIELDS_SQL
            + "WHERE general_planner_host_instance_id = ? AND status = ? AND line_item_end_date_timestamp > ?";

    private static final String GET_LINE_ITEMS_INACTIVE_SINCE_SQL =
            SELECT_COMMON_FIELDS_SQL
            + "WHERE general_planner_host_instance_id = ? AND status != '" + Constants.LINE_ITEM_ACTIVE_STATUS + "' "
            + "AND line_item_end_date_timestamp > ? AND updated_at > ?";

    private static final String GET_COMPACT_LINE_ITEMS_BY_STATUS_SQL =
            "SELECT line_item_id, bidder_code FROM line_items "
            + "WHERE general_planner_host_instance_id = ? AND status = ? AND line_item_end_date_timestamp > ?";

    private static final String UPDATE_LINE_ITEMS_SQL =
            "REPLACE INTO line_items ("
            + "general_planner_host_instance_id, line_item_id, bidder_code, status, "
            + "line_item_start_date_timestamp, line_item_end_date_timestamp, line_item, updated_at) "
            + "VALUES  ";

    private static final String LINE_ITEM_LOG_FORMAT = "{0}::{1}";

    private final Metrics metrics;

    private final AdminTracer tracer;

    private final String hostname;

    public LineItemsClient(String hostname, Metrics metrics, AdminTracer adminTracer) {
        this.hostname = Validators.checkArgument(hostname, !StringUtils.isEmpty(hostname), "hostname is blank");
        this.metrics = Objects.requireNonNull(metrics);
        this.tracer = Objects.requireNonNull(adminTracer);
    }

    Future<UpdateResult> updateLineItems(SQLConnection connection, List<LineItem> lineItems) {
        lineItems.stream().peek(logger::debug);

        StringBuilder sb = StringUtil.appendRepeatedly(new StringBuilder(UPDATE_LINE_ITEMS_SQL),
                "(?, ?, ?, ?, ?, ?, ?, ?)", ",", lineItems.size());

        final Future<UpdateResult> updateResultFuture = Future.future();
        final long start = System.currentTimeMillis();
        final String method = "update-line-items";

        connection.updateWithParams(sb.toString(),
                fillInLineItemsParams(lineItems),
                ar -> {
                    connection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (ar.succeeded()) {
                        if (tracer.checkActive()) {
                            for (LineItem lineItem : lineItems) {
                                trace(lineItem);
                            }
                        }
                        logger.debug("Line items saved successfully");
                    } else {
                        logger.error("Error in persisting line items::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    updateResultFuture.handle(ar);
            });

        return updateResultFuture;
    }

    private JsonArray fillInLineItemsParams(List<LineItem> lineItems) {
        final JsonArray array = new JsonArray();
        for (LineItem lineItem : lineItems) {
            final String json = Json.encode(lineItem.getLineItemJson());
            array.add(this.hostname)
                    .add(lineItem.getLineItemId())
                    .add(lineItem.getBidderCode())
                    .add(lineItem.getStatus())
                    .add(lineItem.getStartTimeStamp())
                    .add(lineItem.getEndTimeStamp())
                    .add(json)
                    .add(lineItem.getUpdatedAt());
        }
        return array;
    }

    Future<List<LineItem>> getLineItemsByStatus(SQLConnection sqlConnection, String status, Instant endTime) {
        final Future<ResultSet> resultSetFuture = Future.future();
        final long start = System.currentTimeMillis();
        final String method = "read-line-items";
        Instant endDate = (endTime == null) ? Instant.now() : endTime;

        if (!tracer.checkActive()) {
            logger.debug("Get line items in status {0} with end date before {1}", status, endDate);
        } else {
            logger.info("{0}::Get line items in status {1} with end date before {2}",
                    GPConstants.TRACER, status, endDate);
        }

        sqlConnection.queryWithParams(GET_LINE_ITEMS_BY_STATUS_SQL,
                new JsonArray().add(hostname).add(status).add(endDate),
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logGetLineItemsError(method, ar.cause());
                    }
                    resultSetFuture.handle(ar);
                });

        return resultSetFuture.map(rs -> mapToLineItems(rs, "getLineItemsByStatus"));
    }

    Future<List<LineItem>> getCompactLineItemsByStatus(SQLConnection sqlConnection, String status, Instant endTime) {
        final Future<ResultSet> resultSetFuture = Future.future();
        final long start = System.currentTimeMillis();
        final String method = "read-compact-line-items";

        sqlConnection.queryWithParams(GET_COMPACT_LINE_ITEMS_BY_STATUS_SQL,
                new JsonArray().add(hostname).add(status).add(endTime == null ? Instant.now() : endTime),
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName("read-compact-line-items"), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logGetLineItemsError(method, ar.cause());
                    }
                    resultSetFuture.handle(ar);
                });

        return resultSetFuture.map(this::mapToCompactLineItems);
    }

    Future<List<LineItem>> getLineItemsInactiveSince(SQLConnection sqlConnection, Instant timestamp) {
        final Future<ResultSet> resultSetFuture = Future.future();
        final long start = System.currentTimeMillis();
        final String method = "read-inactive-line-items";

        if (!tracer.checkActive()) {
            logger.debug("Get line items inactive since {0}", timestamp);
        } else {
            logger.info("{0}::Get line items inactive since {1}", GPConstants.TRACER, timestamp);
        }

        sqlConnection.queryWithParams(GET_LINE_ITEMS_INACTIVE_SINCE_SQL,
                new JsonArray().add(hostname).add(Instant.now()).add(timestamp),
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logGetLineItemsError(method, ar.cause());
                    }
                    resultSetFuture.handle(ar);
                });

        return resultSetFuture.map(rs -> mapToLineItems(rs, "getLineItemsInactiveSince"));
    }

    private void logGetLineItemsError(String method, Throwable cause) {
        logger.error("Failure in reading line_items information {0}", cause.getMessage());
        metrics.incCounter(metricName(method + ".exc"));
    }

    private List<LineItem> mapToLineItems(ResultSet resultSet, String method) {
        final List<LineItem> lineItems = new ArrayList<>();

        if (resultSet.getResults() != null) {
            for (JsonArray ja : resultSet.getResults()) {
                try {
                    LineItem lineItem = LineItem.builder()
                            .bidderCode(ja.getString(1))
                            .lineItemId(ja.getString(2))
                            .lineItemJson(Json.decodeValue(ja.getString(0), ObjectNode.class))
                            .build();
                    lineItems.add(lineItem);
                    trace(lineItem);
                } catch (Exception e) {
                    logger.error("Exception in creating LineItem object::{0}", e.getCause().getMessage());
                    throw new GeneralPlannerException(e);
                }
            }
        }
        if (!tracer.checkActive()) {
            logger.debug("{0} count::{1}", method, lineItems.size());
        } else {
            logger.info("{0}::{1} count::{2}", GPConstants.TRACER, method, lineItems.size());
        }
        return lineItems;
    }

    private void trace(LineItem lineItem) {
        String accountId = JsonUtil.optString(lineItem.getLineItemJson(), Constants.FIELD_ACCOUNT_ID);
        if (tracer.checkActive()
                && tracer.matchBidderCode(lineItem.getBidderCode())
                && tracer.matchLineItemId(lineItem.getLineItemId())
                && tracer.matchAccount(accountId)) {
            logger.info(LINE_ITEM_LOG_FORMAT, GPConstants.TRACER, lineItem.getLineItemJson());
        }
    }

    private List<LineItem> mapToCompactLineItems(ResultSet resultSet) {
        final List<LineItem> lineItems = new ArrayList<>();

        if (resultSet.getResults() != null) {
            for (JsonArray ja : resultSet.getResults()) {
                try {
                    LineItem lineItem = LineItem.builder()
                            .lineItemId(ja.getString(0))
                            .bidderCode(ja.getString(1))
                            .build();
                    lineItems.add(lineItem);
                    if (tracer.checkActive()
                            && tracer.matchBidderCode(lineItem.getBidderCode())
                            && tracer.matchLineItemId(lineItem.getLineItemId())) {
                        logger.info(LINE_ITEM_LOG_FORMAT, GPConstants.TRACER, lineItem.toString());
                    }
                } catch (Exception e) {
                    logger.error("Exception in creating LineItem object::{0}", e.getCause().getMessage());
                    throw new GeneralPlannerException(e);
                }
            }
        }
        metrics.incCounter("counts.active-line-items", lineItems.size());
        logger.debug("getCompactLineItemsByStatus count:: {0}", lineItems.size());
        return lineItems;
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }
}
