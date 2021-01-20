package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.TracerFilters;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@ExtendWith(VertxExtension.class)
public class ReallocatedPlansClientTest extends DataAccessClientTestBase {
    private static final String TABLE = "reallocated_plans";
    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE `reallocated_plans` ("
            + "  `service_instance_id` varchar(64) NOT NULL,"
            + "  `vendor` varchar(64) NOT NULL,"
            + "  `region` varchar(32) NOT NULL,"
            + "  `instance_id` varchar(64) NOT NULL,"
            + "  `token_reallocation_weights` text NOT NULL,"
            + "  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + "  PRIMARY KEY (`service_instance_id`, `vendor`, `region`,`instance_id`))";
    
    private ReallocatedPlansClient dbClient;

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
        AdminTracer tracer = new AdminTracer();
        TracerFilters filters = new TracerFilters();
        filters.setLineItemId("1111");
        filters.setBidderCode("PGVENDOR1");
        filters.setRegion("us-east");
        filters.setVendor("vendor1");
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        tracer.setFilters(filters);
        dbClient =  new ReallocatedPlansClient(HOST_NAME, new Metrics(new MetricRegistry()), tracer);
    }

    private ReallocatedPlan prepareTableData() throws Exception {
        String sqlFormat = "replace into reallocated_plans ("
                + "service_instance_id, vendor, region, instance_id, token_reallocation_weights) "
                + " values ( '%s', '%s', '%s', '%s', '%s' )";
        ReallocatedPlan plan = loadItems("reallocated-plans", "reallocation-plans.json",
                ReallocatedPlan.class).get(0);
        plan.setServiceInstanceId(HOST_NAME);
        String sql = String.format(sqlFormat, HOST_NAME, plan.getVendor(), plan.getRegion(),
                plan.getInstanceId(), Json.mapper.writeValueAsString(plan.getReallocationWeights()));
        connection.createStatement().execute(sql);
        return plan;
    }

    @Test
    void shouldGetReallocatedPlan(VertxTestContext ctx) throws Exception {
        ReallocatedPlan plan = prepareTableData();
        Future<ReallocatedPlan> future = connect().compose(
                sqlConnection -> dbClient.getReallocatedPlan(
                        sqlConnection, plan.getInstanceId(), plan.getRegion(), plan.getVendor()));

        future.setHandler(ctx.succeeding(rs ->
            ctx.verify(() -> {
                assertThat(rs.getUniqueInstanceId(), equalTo(plan.getUniqueInstanceId()));
                assertThat(rs.getReallocationWeights(), equalTo(plan.getReallocationWeights()));
                ctx.completeNow();
            })
        ));
    }

    @Test
    void shouldGetReallocatedPlansFailedWhenMissingTable(VertxTestContext ctx) throws Exception {
        connection.createStatement().execute("DROP TABLE reallocated_plans");
        Future<ReallocatedPlan> future = connect().compose(
                sqlConnection -> dbClient.getReallocatedPlan(
                        sqlConnection, "hostInstanceId", "region", "vendor"));

        future.setHandler(ctx.failing(rs ->
                ctx.verify(() -> {
                    assertThat(rs.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                    ctx.completeNow();
                })
        ));
    }

    @Test
    void shouldUpdateReallocatedPlans(VertxTestContext ctx) throws Exception {
        List<ReallocatedPlan> plans = loadItems("reallocated-plans", "reallocation-plans.json",
                ReallocatedPlan.class);
        Instant updatedAt = Instant.now();
        for (ReallocatedPlan plan : plans) {
            plan.setUpdatedAt(updatedAt);
        }
        Future<List<Integer>> future =  connect().compose(
                sqlConnection -> dbClient.updateReallocatedPlans(sqlConnection, plans));

        future.setHandler(ctx.succeeding(rs ->
            ctx.verify(() -> {
                assertThat(rs.size(), equalTo(1));
                ctx.completeNow();
            })
        ));
    }

    @Test
    void shouldUpdateReallocatedPlansFailedWhenMissingTable(VertxTestContext ctx) throws Exception {
        List<ReallocatedPlan> plans = loadItems("reallocated-plans", "reallocation-plans.json",
                ReallocatedPlan.class);
        connection.createStatement().execute("DROP TABLE reallocated_plans");
        Future<List<Integer>> future =  connect().compose(
                sqlConnection -> dbClient.updateReallocatedPlans(sqlConnection, plans));

        future.setHandler(ctx.failing(rs ->
                ctx.verify(() -> {
                    assertThat(rs.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                    ctx.completeNow();
                })
        ));
    }

    @Test
    void shouldGetLatestReallocatedPlans(VertxTestContext ctx) throws Exception {
        ReallocatedPlan plan = prepareTableData();
        Instant updatedSince = Instant.now().minus(15, ChronoUnit.MINUTES);
        Future<List<ReallocatedPlan>> future =
                connect().compose(sqlConnection -> dbClient.getLatestReallocatedPlans(sqlConnection, updatedSince));

        future.setHandler(ctx.succeeding(rs ->
                ctx.verify(() -> {
                    assertThat(rs.size(), equalTo(1));
                    ReallocatedPlan reallocatedPlan = rs.get(0);
                    assertThat(reallocatedPlan.getUniqueInstanceId(), equalTo(plan.getUniqueInstanceId()));
                    assertThat(reallocatedPlan.getReallocationWeights(), equalTo(plan.getReallocationWeights()));
                    ctx.completeNow();
                })
        ));
    }
 }
