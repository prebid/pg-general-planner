package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.PageRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

@ExtendWith(VertxExtension.class)
public class LineItemsTokensSummaryClientTest extends DataAccessClientTestBase {

    private static final String TABLE = "line_items_tokens_summary";

    private String bidderCode = "bidderCode1";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE `line_items_tokens_summary` ("
            + "  `id` int(11) NOT NULL AUTO_INCREMENT,"
            + "  `summary_window_start_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "  `summary_window_end_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "  `line_item_id` varchar(64) NOT NULL,"
            + "  `bidder_code` varchar(64) NOT NULL,"
            + "  `ext_line_item_id` varchar(64) NOT NULL,"
            + "  `tokens` int(11) NOT NULL DEFAULT '0',"
            + "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "  PRIMARY KEY (`id`))";

    private LineItemsTokensSummaryClient tokensSummaryClient;

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
        alertHttpClientMock = mock(AlertProxyHttpClient.class);
        tokensSummaryClient = new LineItemsTokensSummaryClient(jdbcClient, metrics, alertHttpClientMock);
    }

    @Test
    void shouldGetLineItemsTokensSummaryCount(VertxTestContext context) throws Exception {
        String lineItemId = "l1";
        populateTableData(lineItemId);
        Instant start = Instant.parse("2020-03-01T07:00:00.000Z");
        Instant end = Instant.parse("2020-03-01T09:00:00.000Z");
        List<String> ids = Arrays.asList(lineItemId);
        Future<Integer> future = connect().compose(sqlConnection ->
                tokensSummaryClient.getLineItemsTokensSummaryCount(sqlConnection, start, end, ids));
        future.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.intValue(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetLineItemsTokensSummary(VertxTestContext context) throws Exception {
        String lineItemId = "l1";
        populateTableData(lineItemId);
        Instant start = Instant.parse("2020-03-01T07:00:00.000Z");
        Instant end = Instant.parse("2020-03-01T09:00:00.000Z");
        List<String> ids = Arrays.asList(lineItemId);
        PageRequest pageRequest = PageRequest.builder().number(0).size(1).build();
        Future<List<LineItemsTokensSummary>> future = connect().compose(sqlConnection ->
                tokensSummaryClient.getLineItemsTokensSummary(sqlConnection, start, end, ids, pageRequest));
        future.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.size(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldSaveLineItemsTokenSummary(VertxTestContext context) {
        List<LineItemsTokensSummary> summaries = new ArrayList<>();
        summaries.add(LineItemsTokensSummary.builder()
                .summaryWindowStartTimestamp(Instant.parse("2020-03-01T07:00:00.000Z"))
                .summaryWindowEndTimestamp(Instant.parse("2020-03-01T09:00:00.000Z"))
                .tokens(40)
                .extLineItemId("extL1")
                .bidderCode(bidderCode)
                .lineItemId("l1")
                .createdAt(Instant.parse("2020-03-01T08:00:00.000Z"))
                .build());
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> tokensSummaryClient.saveLineItemsTokenSummary(summaries));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.getUpdated(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldSaveLineItemsTokenSummaryFailed(VertxTestContext context) {
        List<LineItemsTokensSummary> summaries = new ArrayList<>();
        summaries.add(LineItemsTokensSummary.builder()
                .summaryWindowStartTimestamp(Instant.parse("2020-03-01T07:00:00.000Z"))
                .summaryWindowEndTimestamp(Instant.parse("2020-03-01T09:00:00.000Z"))
                .tokens(40)
                .extLineItemId("extL1")
                .bidderCode(bidderCode)
                .lineItemId("l1")
                .build());
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> tokensSummaryClient.saveLineItemsTokenSummary(summaries));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), IsEqual.equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    private void populateTableData(String lineItemId) throws Exception {
        String sqlFormat = "replace into line_items_tokens_summary ("
                + "id, summary_window_start_timestamp, summary_window_end_timestamp, line_item_id, bidder_code, "
                + "ext_line_item_id, tokens) "
                + "values('%s', '%s', '%s', '%s', '%s', '%s', '%s')";
        String sql1 = String.format(sqlFormat, 1, "2020-03-01T06:00:00.000Z", "2020-03-01T07:00:00.000Z",
                lineItemId, bidderCode, "l1", 40);
        String sql2 = String.format(sqlFormat, 2, "2020-03-01T07:00:00.000Z", "2020-03-01T08:00:00.000Z",
                lineItemId, bidderCode, "l1", 30);
        String sql3 = String.format(sqlFormat, 3, "2020-03-01T08:00:00.000Z", "2020-03-01T09:00:00.000Z",
                lineItemId, bidderCode, "l1", 20);
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        connection.createStatement().execute(sql3);
    }
}
