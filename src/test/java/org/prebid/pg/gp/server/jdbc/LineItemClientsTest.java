package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.TracerFilters;
import org.prebid.pg.gp.server.util.Constants;

import java.io.File;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

@ExtendWith(VertxExtension.class)
public class LineItemClientsTest extends DataAccessClientTestBase {
    private static final String TABLE = "line_items";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE `line_items` ("
            + "  `general_planner_host_instance_id` varchar(64) NOT NULL,"
            + "  `line_item_id` varchar(64) NOT NULL,"
            + "  `bidder_code` varchar(32) NOT NULL,"
            + "  `status` varchar(16) NOT NULL,"
            + "  `line_item_start_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "  `line_item_end_date_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "  `line_item` clob NOT NULL,"
            + "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + "  PRIMARY KEY (`general_planner_host_instance_id`,`line_item_id`,`bidder_code`))";

    private LineItemsClient dbClient;

    private AdminTracer tracer;

    private Metrics metricsMock;

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
        metricsMock = mock(Metrics.class);
        tracer = new AdminTracer();
        TracerFilters filters = new TracerFilters();
        filters.setLineItemId("456");
        filters.setBidderCode("PGVENDOR1");
        filters.setAccountId("1001");
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        tracer.setFilters(filters);
        dbClient = new LineItemsClient(HOST_NAME, new Metrics(new MetricRegistry()), tracer);
    }

    @AfterEach
    void cleanUpAfterEach() throws Exception {
        try {
            connection.createStatement().execute("DROP TABLE line_items;");
        } catch(Exception ex) {
            // do nothing
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldUpdateLineItems(VertxTestContext context) {
        ArrayList<LineItem> lis = new ArrayList<>();
        lis.add(lineItem());
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> dbClient.updateLineItems(sqlConnection, lis));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.getUpdated(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldUpdateLineItemsOnMissingTable(VertxTestContext context) throws SQLException {
        connection.createStatement().execute("DROP TABLE line_items");

        ArrayList<LineItem> lis = new ArrayList<>();
        lis.add(lineItem());
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> dbClient.updateLineItems(sqlConnection, lis));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetLineItemsByStatusOnMissingTable(VertxTestContext context) throws Exception {
        connection.createStatement().execute("DROP TABLE line_items");
        Instant endTime = Instant.now().plusSeconds(3600);

        ArrayList<LineItem> lis = new ArrayList<>();
        lis.add(lineItem());
        Future<List<LineItem>> future = connect().compose(
                sqlConnection -> dbClient.getLineItemsByStatus(sqlConnection, "active", endTime));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetCompactLineItemsByStatusOnMissingTable(VertxTestContext context) throws Exception {
        connection.createStatement().execute("DROP TABLE line_items");
        Instant endTime = Instant.now().plusSeconds(3600);
        ArrayList<LineItem> lis = new ArrayList<>();
        lis.add(lineItem());
        Future<List<LineItem>> future = connect().compose(
                sqlConnection -> dbClient.getCompactLineItemsByStatus(sqlConnection, "active", endTime));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetLineItemsInactiveSinceOnMissingTable(VertxTestContext context) throws Exception {
        connection.createStatement().execute("DROP TABLE line_items");

        ArrayList<LineItem> lis = new ArrayList<>();
        lis.add(lineItem());
        Future<List<LineItem>> future = connect().compose(
                sqlConnection -> dbClient.getLineItemsInactiveSince(sqlConnection, Instant.now()));
        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldGetLineItemsByStatusSuccessfullyWhenTracerEnabled(VertxTestContext ctx) throws Exception {
        shouldGetLineItemsByStatusSuccessfully(ctx);
    }

    @Test
    void shouldGetLineItemsByStatusSuccessfullyWhenTracerDisabled(VertxTestContext ctx) throws Exception {
        tracer.setEnabled(false);
        shouldGetLineItemsByStatusSuccessfully(ctx);
    }

    void shouldGetLineItemsByStatusSuccessfully(VertxTestContext ctx) throws Exception {
        List<LineItem> lis = loadLineItems("line-items.json");
        saveLineItem(lis.get(0));
        List<LineItem> expectedLineItems = new ArrayList<>();
        expectedLineItems.add(lis.get(0));

        Future<List<LineItem>> future = connect().compose(sqlConnection ->
                dbClient.getLineItemsByStatus(sqlConnection, Constants.LINE_ITEM_ACTIVE_STATUS, null));
        future.setHandler(ctx.succeeding(lineItems ->
                ctx.verify(() -> {
                    assertLineItems(lineItems, expectedLineItems);
                    ctx.completeNow();
                })
        ));
    }

    @Test
    void shouldGetCompactLineItemsByStatus(VertxTestContext ctx) throws Exception {
        List<LineItem> lis = loadLineItems("line-items.json");
        saveLineItem(lis.get(0));
        List<LineItem> expectedLineItems = new ArrayList<>();
        expectedLineItems.add(lis.get(0));

        Future<List<LineItem>> future = connect().compose(
                sqlConnection -> dbClient.getCompactLineItemsByStatus(sqlConnection, Constants.LINE_ITEM_ACTIVE_STATUS, null));
        future.setHandler(ctx.succeeding(lineItems ->
                ctx.verify(() -> {
                    for (int i = 0; i < lineItems.size(); i++) {
                        LineItem actual = lineItems.get(i);
                        LineItem expected = expectedLineItems.get(i);
                        assertThat(actual.getLineItemId(), equalTo(expected.getLineItemId()));
                        assertThat(actual.getBidderCode(), equalTo(expected.getBidderCode()));
                        assertThat(actual.getLineItemJson(), is(nullValue()));
                    }
                    ctx.completeNow();
                })
        ));
    }

    private void assertLineItems(List<LineItem> actualLineItems, List<LineItem> expectedLineItems) {
        assertThat(actualLineItems.size(), equalTo(expectedLineItems.size()));
        for (int i= 0; i < actualLineItems.size(); i++) {
            LineItem actual = actualLineItems.get(i);
            LineItem expected = expectedLineItems.get(i);
            assertThat(actual.getLineItemId(), equalTo(expected.getLineItemId()));
            assertThat(actual.getBidderCode(), equalTo(expected.getBidderCode()));
            assertThat(actual.getLineItemJson(), equalTo(expected.getLineItemJson()));
        }
    }

    @Test
    void shouldGetLineItemInactiveSinceWhenTracerEnabled(VertxTestContext ctx) throws Exception {
        shouldGetLineItemInactiveSinceSuccessfully(ctx);
    }

    @Test
    void shouldGetLineItemInactiveSinceWhenTracerDisabled(VertxTestContext ctx) throws Exception {
        tracer.setEnabled(false);
        shouldGetLineItemInactiveSinceSuccessfully(ctx);
    }

    private void shouldGetLineItemInactiveSinceSuccessfully(VertxTestContext ctx) throws Exception {
        List<LineItem> lis = loadLineItems("line-items.json");
        saveLineItem(lis.get(1));
        List<LineItem> expectedLineItems = new ArrayList<>();
        expectedLineItems.add(lis.get(1));

        Future<List<LineItem>> future = connect().compose(
                sqlConnection -> dbClient.getLineItemsInactiveSince(sqlConnection, Instant.now().minusSeconds(6400)));
        future.setHandler(ctx.succeeding(lineItems ->
                ctx.verify(() -> {
                    assertLineItems(lineItems, expectedLineItems);
                    ctx.completeNow();
                })
        ));
    }

    private void saveLineItem(LineItem li) throws Exception {
        String sqlFormat = "replace into line_items ("
                + "general_planner_host_instance_id, line_item_id, bidder_code, status, "
                + "line_item_start_date_timestamp, line_item_end_date_timestamp, line_item) "
                + "values ('%s', '%s', '%s', '%s', '%s', '%s', '%s')";
        String sql = String.format(sqlFormat, HOST_NAME, li.getLineItemId(), li.getBidderCode(),
                li.getStatus(), li.getStartTimeStamp(), li.getEndTimeStamp(),
                Json.mapper.writeValueAsString(li.getLineItemJson()));
        connection.createStatement().execute(sql);
    }

    private List<LineItem> loadLineItems(String fileName) throws Exception {
        List<LineItem> lis = new ArrayList<>();
        String path = "line-item-clients/" +  fileName;
        final File statsFile = new File(Resources.getResource(path).toURI());
        final String content = FileUtils.readFileToString(statsFile, "UTF-8");
        List<ObjectNode> nodes = Json.mapper.readValue(content, new TypeReference<List<ObjectNode>>(){});
        for (ObjectNode node : nodes) {
            lis.add(LineItem.from(node, "vendor1", "pg"));
        }
        return lis;
    }

    private LineItem lineItem() {
        LineItem li = LineItem.builder().build();
        li.setBidderCode("pgvendor1");
        li.setLineItemId("1");
        li.setStatus(Constants.LINE_ITEM_ACTIVE_STATUS);
        li.setStartTimeStamp("2019-01-01T03:00:00Z");
        li.setEndTimeStamp("2019-12-01T03:00:00Z");
        li.setUpdatedAt(Instant.now());
        return li;
    }
}
