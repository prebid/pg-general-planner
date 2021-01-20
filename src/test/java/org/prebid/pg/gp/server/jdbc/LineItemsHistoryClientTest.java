package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.LineItemIdentity;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

@ExtendWith(VertxExtension.class)
public class LineItemsHistoryClientTest extends DataAccessClientTestBase {

    private static final String TABLE = "line_items_history";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE `line_items_history` ("
            + " `audit_id` int(11) NOT NULL AUTO_INCREMENT,"
            + " `general_planner_host_instance_id` varchar(64) NOT NULL,"
            + " `line_item_id` varchar(64) NOT NULL,"
            + " `bidder_code` varchar(32) NOT NULL,"
            + " `status` varchar(16) NOT NULL,"
            + " `line_item_start_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + " `line_item_end_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + " `line_item` clob NOT NULL,"
            + " `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + " `audit_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + " PRIMARY KEY (`audit_id`))";

    private LineItemsHistoryClient historyClient;

    private SystemStateClient systemStateClient;

    private AlertProxyHttpClient alertHttpClientMock;

    @Override
    protected String getCreateTableSql() {
        return CREATE_TABLE_SQL;
    }

    @Override
    protected String getTableName() {
        return TABLE;
    }

    @Override
    protected void beforeEach() {
        Metrics metrics = new Metrics(new MetricRegistry());
        systemStateClient = new SystemStateClient(metrics);
        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        historyClient = new LineItemsHistoryClient(jdbcClient, metrics, systemStateClient, alertHttpClientMock);
    }

    @Test
    void shouldFindLineItemsTokens(VertxTestContext context) throws Exception {
        String planStart = "2019-10-03T11:50:00Z";
        String planEnd = "2019-10-03T11:55:00Z";
        String updatedAt = "2019-10-03T11:48:00Z";
        saveRecord(planStart, planEnd, updatedAt);
        Instant intervalStart = Instant.parse(updatedAt).truncatedTo(ChronoUnit.HOURS);
        Future<List<LineItemsTokensSummary>> future =
                historyClient.findLineItemTokens(intervalStart, intervalStart.plus(1, ChronoUnit.HOURS));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.size(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldFindLineItemsTokensWithEarlierStartTimestamp(VertxTestContext context) throws Exception {
        String planStart = "2019-10-03T11:50:00Z";
        String planEnd = "2019-10-03T11:55:00Z";
        String updatedAt = "2019-10-03T11:48:00Z";
        saveRecord(planStart, planEnd, updatedAt);
        // DB query filter out
        Instant intervalStart = Instant.parse(updatedAt).truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
        Future<List<LineItemsTokensSummary>> future =
                historyClient.findLineItemTokens(intervalStart, intervalStart.plus(1, ChronoUnit.HOURS));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.size(), equalTo(0));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldFindLineItemsTokensWithEarlierStartTimestamp2(VertxTestContext context) throws Exception {
        String planStart = "2019-10-03T11:50:00Z";
        String planEnd = "2019-10-03T11:55:00Z";
        String updatedAt = "2019-10-03T12:02:00Z";
        saveRecord(planStart, planEnd, updatedAt);
        Instant intervalStart = Instant.parse(updatedAt).truncatedTo(ChronoUnit.HOURS);
        // in memory filter out
        Future<List<LineItemsTokensSummary>> future =
                historyClient.findLineItemTokens(intervalStart, intervalStart.plus(1, ChronoUnit.HOURS));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.size(), equalTo(0));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldBuildQuerySql() {
        List<LineItemIdentity> ids = new ArrayList<>();
        String expectedSQL = "SELECT bidder_code, line_item_id, line_item, updated_at "
        + "FROM line_items_history "
        + "WHERE updated_at >= ? AND updated_at < ? ORDER BY updated_at ASC";
        String sql = historyClient.buildQuerySql(ids);
        assertThat(sql, equalTo(expectedSQL));

        ids.add(LineItemIdentity.builder().bidderCode("bidder1").lineItemId("l1").build());
        expectedSQL = "SELECT bidder_code, line_item_id, line_item, updated_at FROM line_items_history "
        + "WHERE updated_at >= ? AND updated_at < ?  AND ((bidder_code = 'bidder1' AND line_item_id = 'l1')) "
        + "ORDER BY updated_at ASC";
        sql = historyClient.buildQuerySql(ids);
        assertThat(sql, equalTo(expectedSQL));

        ids.add(LineItemIdentity.builder().bidderCode("bidder2").lineItemId("l2").build());
        expectedSQL = "SELECT bidder_code, line_item_id, line_item, updated_at FROM line_items_history "
        + "WHERE updated_at >= ? AND updated_at < ?  "
        + "AND ((bidder_code = 'bidder1' AND line_item_id = 'l1') OR (bidder_code = 'bidder2' AND line_item_id = 'l2')) "
        + "ORDER BY updated_at ASC";
        sql = historyClient.buildQuerySql(ids);
        assertThat(sql, equalTo(expectedSQL));
    }

    private void saveRecord(String start, String end, String updatedAt) throws Exception {
        String sqlFormat = "replace into line_items_history ("
                + "audit_id, general_planner_host_instance_id, line_item_id, bidder_code, status, "
                + "line_item_start_date_timestamp, line_item_end_date_timestamp, line_item, updated_at, audit_time) "
                + "values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
        String jsonFormat =
                "{\n"
                        + "  \"lineItemId\": \"22\",\n"
                        + "  \"deliverySchedules\": [\n"
                        + "    {\n"
                        + "      \"planId\": \"35440897\",\n"
                        + "      \"tokens\": [\n"
                        + "        {\n"
                        + "          \"class\": 1,\n"
                        + "          \"total\": 40\n"
                        + "        }\n"
                        + "      ],\n"
                        + "      \"endTimeStamp\": \"%s\",\n"
                        + "      \"startTimeStamp\": \"%s\"\n"
                        + "    }"
                        + " ]\n"
                        + "}";
        String json = String.format(jsonFormat, end, start);
        String sql = String.format(sqlFormat, 1, "host", "22", "bidder", "active", start, end, json,
                updatedAt, updatedAt);
        connection.createStatement().execute(sql);
    }

}