package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary.SummaryData;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary.TokenSpend;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@ExtendWith(VertxExtension.class)
public class TokenSpendClientTest extends DataAccessClientTestBase {
    private static final String TABLE = "delivery_token_spend_summary";

    private static final String CREATE_TABLE_SQL =
            "create table `delivery_token_spend_summary`("
            + "  `vendor` varchar(32) NOT NULL,"
            + "  `region` varchar(32) NOT NULL,"
            + "  `instance_id` varchar(128) NOT NULL,"
            + "  `ext_line_item_id` varchar(72) NOT NULL,"
            + "  `line_item_id` varchar(36) NOT NULL,"
            + "  `data_window_start_timestamp` timestamp NOT NULL,"
            + "  `data_window_end_timestamp` timestamp NOT NULL,"
            + "  `report_timestamp` timestamp NOT NULL,"
            + "  `service_instance_id` varchar(128) DEFAULT NULL,"
            + "  `summary_data` text NOT NULL,"
            + "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + "  PRIMARY KEY (`vendor`,`region`,`instance_id`,`ext_line_item_id`))";
    private TokenSpendClient tokenSpendClient;

    private String vendor = "vendor1";

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
        tokenSpendClient = new TokenSpendClient(HOST_NAME, new Metrics(new MetricRegistry()));
    }

    @Test
    public void shouldUpdateTokenSpendData(VertxTestContext context) {
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> tokenSpendClient.updateTokenSpendData(sqlConnection, deliveryTokenSpendSummaries()));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.getUpdated(), equalTo(2));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetTokenSpendDataWhenTableIsEmpty(VertxTestContext context) {
        Future<List<DeliveryTokenSpendSummary>> future = connect().compose(
                sqlConnection -> tokenSpendClient.getTokenSpendData(sqlConnection, "hostInstanceId",
                        "region", "vendor", Instant.now()));
        future.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.isEmpty(), Is.is(true));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetTokenSpendDataFailedOnMissingTable(VertxTestContext context) throws SQLException {
        connection.createStatement().execute("DROP TABLE delivery_token_spend_summary");

        Future<List<DeliveryTokenSpendSummary>> future = connect().compose(
                sqlConnection -> tokenSpendClient.getTokenSpendData(sqlConnection, "hostInstanceId",
                        "region", "vendor", Instant.now()));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    public void shouldUpdateTokenSpendDataOnMissingTable(VertxTestContext context) throws SQLException {
        connection.createStatement().execute("DROP TABLE delivery_token_spend_summary");

        Future<UpdateResult> future = connect().compose(
                sqlConnection -> tokenSpendClient.updateTokenSpendData(sqlConnection, deliveryTokenSpendSummaries()));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    public void shouldUpdateTokenSpendDataOnPartialFailure(VertxTestContext context) throws SQLException {
        List<DeliveryTokenSpendSummary> summaries = partialFailedTokenSpendSummaries();
        Future<UpdateResult> future = connect()
                .compose(sqlConnection -> tokenSpendClient.updateTokenSpendData(sqlConnection, summaries));

        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));

        VertxTestContext ctx = new VertxTestContext();
        final Future<Integer> cntFuture = connect().compose(sqlConnection -> getTableRowCount(sqlConnection));
        cntFuture.setHandler(context.succeeding(rs -> {
            ctx.verify(() -> {
                assertThat(rs.intValue(), equalTo(1));
                ctx.completeNow();
            });
        }));
    }

    @Test
    public void shouldGetTokenSpendDataSuccessfully(VertxTestContext ctx) throws Exception {
        final DeliveryTokenSpendSummary summary = deliveryTokenSummary("rpt-1", vendor);
        save(summary);
        Future<List<DeliveryTokenSpendSummary>> future = connect().compose(sqlConnection ->
                tokenSpendClient.getTokenSpendData(sqlConnection, summary.getInstanceId(), summary.getRegion(),
                        summary.getVendor(), Instant.now().minusSeconds(9000)));
        future.setHandler(ctx.succeeding(rs -> {
            ctx.verify(() -> {
                assertThat(rs.size(), equalTo(1));
                DeliveryTokenSpendSummary rsStats = rs.get(0);
                assertThat(rsStats.getUniqueInstanceId(), equalTo(summary.getUniqueInstanceId()));
                assertThat(rsStats.getLineItemId(), equalTo(summary.getLineItemId()));
                ctx.completeNow();
            });
        }));
    }

    private void save(DeliveryTokenSpendSummary summary) throws Exception {
        String sqlFormat = "replace into delivery_token_spend_summary ("
                + "vendor, region, instance_id, ext_line_item_id, line_item_id, data_window_start_timestamp, "
                + "data_window_end_timestamp, report_timestamp, service_instance_id, summary_data) "
                + "values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";

        String summaryData = "{\n" +
                "        \"targetMatched\": 16,\n" +
                "        \"tokenSpent\": [\n" +
                "          {\n" +
                "            \"pc\": 102,\n" +
                "            \"class\": 1\n" +
                "          },\n" +
                "          {\n" +
                "            \"pc\": 80,\n" +
                "            \"class\": 2\n" +
                "          }\n" +
                "        ]\n" +
                "      }";
        String sql = String.format(sqlFormat, summary.getVendor(), summary.getRegion(),
                summary.getInstanceId(), summary.getExtLineItemId(), summary.getLineItemId(),
                summary.getDataWindowStartTimestamp(), summary.getDataWindowEndTimestamp(),
                summary.getReportTimestamp(), summary.getServiceInstanceId(), summaryData);
        connection.createStatement().execute(sql);
    }

    private Future<Integer> getTableRowCount(SQLConnection connection) {
        final String readSql = "select count(*) from delivery_token_spend_summary";
        final Future<ResultSet> resultFuture = Future.future();
        connection.query(readSql,
                ar -> {
                    connection.close();
                    resultFuture.handle(ar);
                });
        return resultFuture.map(rs -> rs == null ? 0 : rs.getRows().size());
    }

    private DeliveryTokenSpendSummary deliveryTokenSummary(String reportId, String vendor) {
        List<TokenSpend> summaries = new ArrayList<>();
        summaries.add(TokenSpend.builder().pc(80).clazz(10).build());
        return DeliveryTokenSpendSummary.builder()
                .reportId(reportId)
                .vendor(vendor)
                .region("east-1")
                .instanceId("instance-1")
                .lineItemId("pgvendor1-li-1")
                .extLineItemId("li-1")
                .dataWindowStartTimestamp("2019-02-01T07:13:00Z")
                .dataWindowEndTimestamp("2019-02-01T07:13:00Z")
                .reportTimestamp("2019-02-01T07:13:00Z")
                .serviceInstanceId(HOST_NAME)
                .summaryData(SummaryData.builder().tokenSpent(summaries).build())
                .updatedAt(Instant.now())
                .build();
    }

    private List<DeliveryTokenSpendSummary> deliveryTokenSpendSummaries() {
        List<DeliveryTokenSpendSummary> summaries = new ArrayList<>();
        summaries.add(deliveryTokenSummary("rpt-1", vendor));
        summaries.add(deliveryTokenSummary("rpt-2", vendor));
        return summaries;
    }

    private List<DeliveryTokenSpendSummary> partialFailedTokenSpendSummaries() {
        List<DeliveryTokenSpendSummary> summaries = new ArrayList<>();
        summaries.add(deliveryTokenSummary("rpt-1", vendor));
        summaries.add(deliveryTokenSummary("rpt-2", null));
        return summaries;
    }
}

