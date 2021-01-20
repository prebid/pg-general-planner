package org.prebid.pg.gp.server.jdbc;

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
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary.SummaryData;
import org.prebid.pg.gp.server.util.StringUtil;
import org.prebid.pg.gp.server.util.Validators;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A client to access database for line item delivery statistics information.
 */
public class TokenSpendClient {
    private static final Logger logger = LoggerFactory.getLogger(TokenSpendClient.class);

    private static final String GET_DELIVERY_TOKEN_SPEND_SUMMARY_SQL =
            "SELECT ext_line_item_id, line_item_id, summary_data, vendor, data_window_start_timestamp, "
            + "data_window_end_timestamp, report_timestamp, updated_at, report_timestamp "
            + "FROM delivery_token_spend_summary "
            + "WHERE service_instance_id = ? AND instance_id = ? AND region = ? AND vendor = ? AND updated_at > ? ";

    private static final String REPLACE_DELIVERY_TOKEN_SPEND_SUMMARY_SQL =
            "REPLACE INTO delivery_token_spend_summary "
            + "(vendor, region, instance_id, ext_line_item_id, line_item_id, data_window_start_timestamp, "
            + "data_window_end_timestamp, report_timestamp, service_instance_id, summary_data, updated_at) "
            + "VALUES ";

    private final String hostname;

    private final Metrics metrics;

    public TokenSpendClient(String hostname, Metrics metrics) {
        this.hostname = Validators.checkArgument(
                hostname, !StringUtils.isEmpty(hostname), "hostname should not be blank");
        this.metrics = metrics;
    }

    Future<UpdateResult> updateTokenSpendData(
            SQLConnection connection, List<DeliveryTokenSpendSummary> tokenSpendSummaries) {

        tokenSpendSummaries.stream().peek(logger::info);
        logger.debug(REPLACE_DELIVERY_TOKEN_SPEND_SUMMARY_SQL);

        StringBuilder sb = StringUtil.appendRepeatedly(new StringBuilder(REPLACE_DELIVERY_TOKEN_SPEND_SUMMARY_SQL),
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ",", tokenSpendSummaries.size());

        final Future<UpdateResult> updateResultFuture = Future.future();

        final long start = System.currentTimeMillis();
        final String method = "update-token-data-batch";
        connection.updateWithParams(sb.toString(),
                fillInTokenSpendSummariesParams(tokenSpendSummaries),
                ar -> {
                    connection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (ar.succeeded()) {
                        logger.debug(
                                "Persisted {0} DeliveryTokenSpendSummaries successfully",
                                tokenSpendSummaries.size()
                        );
                    } else {
                        logger.error("Error in persisting delivery stats::{0}",
                                ar.cause() == null ? "" : ar.cause().getMessage());
                        metrics.incCounter(metricName(method) + ".exc");
                    }
                    updateResultFuture.handle(ar);
                });
        return updateResultFuture;
    }

    private JsonArray fillInTokenSpendSummariesParams(List<DeliveryTokenSpendSummary> tokenSpendSummaries) {
        JsonArray params = new JsonArray();
        for (DeliveryTokenSpendSummary tokenSpendSummary : tokenSpendSummaries) {
            params.add(tokenSpendSummary.getVendor())
                    .add(tokenSpendSummary.getRegion())
                    .add(tokenSpendSummary.getInstanceId())
                    .add(tokenSpendSummary.getExtLineItemId())
                    .add(tokenSpendSummary.getLineItemId())
                    .add(tokenSpendSummary.getDataWindowStartTimestamp())
                    .add(tokenSpendSummary.getDataWindowEndTimestamp())
                    .add(tokenSpendSummary.getReportTimestamp())
                    .add(this.hostname)
                    .add(Json.encode(tokenSpendSummary.getSummaryData()))
                    .add(tokenSpendSummary.getUpdatedAt());
        }
        return params;
    }

    Future<List<DeliveryTokenSpendSummary>> getTokenSpendData(
            SQLConnection connection, String hostInstanceId, String region, String vendor, Instant updatedSince) {

        JsonArray j = new JsonArray().add(hostname).add(hostInstanceId).add(region).add(vendor).add(updatedSince);
        logger.debug("getTokenSpendData::updatedSince:{0}", updatedSince);

        final Future<ResultSet> resultSetFuture = Future.future();
        final long start = System.currentTimeMillis();
        final String method = "get-token-data";
        connection.queryWithParams(
                GET_DELIVERY_TOKEN_SPEND_SUMMARY_SQL, j,
                ar -> {
                    connection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logger.info("Error in reading token spend data::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method) + ".exc");
                    }
                    resultSetFuture.handle(ar);
                });

        return resultSetFuture.map(rs -> mapToTokenSpendSummaries(rs, hostInstanceId, region));
    }

    private List<DeliveryTokenSpendSummary> mapToTokenSpendSummaries(
            ResultSet resultSet, String hostInstanceId, String region) {
        List<DeliveryTokenSpendSummary> tokenSpendSummaries = new ArrayList<>();

        if (resultSet.getResults() == null || resultSet.getResults().isEmpty()) {
            return tokenSpendSummaries;
        }
        Map<DeliveryTokenSpendSummary, DeliveryTokenSpendSummary> statsMap = new HashMap<>();
        for (JsonArray rs : resultSet.getResults()) {
            try {
                DeliveryTokenSpendSummary summary = DeliveryTokenSpendSummary.builder()
                        .extLineItemId(rs.getString(0))
                        .lineItemId(rs.getString(1))
                        .summaryData(Json.mapper.readValue(rs.getString(2), SummaryData.class))
                        .vendor(rs.getString(3))
                        .region(region)
                        .instanceId(hostInstanceId)
                        .bidderCode(extractBidderCode(rs.getString(1), rs.getString(0)))
                        .dataWindowStartTimestamp(rs.getInstant(4).toString())
                        .dataWindowEndTimestamp(rs.getInstant(5).toString())
                        .reportTimestamp(rs.getInstant(6).toString())
                        .updatedAt(rs.getInstant(7))
                        .reportTime(rs.getInstant(8))
                        .build();

                if (statsMap.containsKey(summary)) {
                    DeliveryTokenSpendSummary existing = statsMap.get(summary);
                    if (existing.getReportTime().isBefore(summary.getReportTime())) {
                        statsMap.put(summary, summary);
                        logger.debug("duplicated::new::{0}.existing::{1}.Drop existing", summary, existing);
                    } else {
                        logger.debug("duplicated::new::{0}.existing::{1}.Keep existing", summary, existing);
                    }
                } else {
                    statsMap.put(summary, summary);
                }
            } catch (Exception e) {
                throw new GeneralPlannerException(e);
            }
        }

        tokenSpendSummaries.addAll(statsMap.keySet());
        logger.debug("TokenSpendClient={0}", tokenSpendSummaries);
        logger.debug("getTokenSpendData::ResultSet_count::{0}|Filtered_count::{1}",
                resultSet.getResults().size(), tokenSpendSummaries.size());
        return tokenSpendSummaries;
    }

    private String extractBidderCode(String lineItemId, String extLineItemId) {
        int index = lineItemId.lastIndexOf("-" + extLineItemId);
        return index > 0 ? lineItemId.substring(0, index) : "";
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }
}
