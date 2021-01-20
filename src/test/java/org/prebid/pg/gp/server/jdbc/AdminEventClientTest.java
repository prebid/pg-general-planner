package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminCommand;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AdminEvent.Directive;
import org.prebid.pg.gp.server.model.Registration;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class AdminEventClientTest extends DataAccessClientTestBase {

    private static final String TABLE = "admin_event";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE `admin_event` ("
            + " `id` varchar(36) NOT NULL,"
            + " `app_name` varchar(64) NOT NULL,"
            + " `vendor` varchar(64) NOT NULL,"
            + " `region` varchar(32) NOT NULL,"
            + " `instance_id` varchar(64) NOT NULL,"
            + " `directive` clob NOT NULL,"
            + " `expiry_at` timestamp NOT NULL,"
            + " `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + " PRIMARY KEY (`id`))";

    private AdminEventClient adminClient;

    private String createdAt = "2020-06-01T00:00:00.000Z";

    private String expiryAt = "2020-06-01T00:05:00.000Z";

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
        Metrics metrics = new Metrics(new MetricRegistry());
        adminClient = new AdminEventClient(metrics);
    }

    @Test
    void shouldFindEarliestActiveAdminEvent(VertxTestContext context) throws Exception {
        prepareTableData();
        Registration registration = Registration.builder()
                .vendor(vendor)
                .region("east")
                .instanceId("host1")
                .build();
        Instant time = Instant.parse(expiryAt).minus(10, ChronoUnit.MINUTES);
        Future<AdminEvent> future = connect().compose(
            sqlConnection -> adminClient.findEarliestActiveAdminEvent(sqlConnection, "pbs", registration, time));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.getId(), equalTo("uuid1"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldDeleteAdminEvent(VertxTestContext context) throws Exception {
        prepareTableData();
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> adminClient.deleteAdminEvent(sqlConnection, "uuid1"));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.getUpdated(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldUpdateAdminEvents(VertxTestContext context) {
        List<AdminEvent> list = new ArrayList<>();
        list.add(adminEvent());
        Future<UpdateResult> future = connect().compose(
                sqlConnection -> adminClient.updateAdminEvents(sqlConnection, list));
        future.setHandler(context.succeeding(rs -> {
            context.verify(() -> {
                assertThat(rs.getUpdated(), IsEqual.equalTo(1));
                context.completeNow();
            });
        }));
    }

    private AdminEvent adminEvent() {
        AdminCommand cmd = new AdminCommand();
        cmd.setCmd("start");
        return AdminEvent.builder()
                .id("uuid1")
                .app("pbs")
                .vendor(vendor)
                .region("east")
                .instanceId("host1")
                .directive(Directive.builder()
                        .services(cmd)
                        .build())
                .expiryAt(Instant.parse(expiryAt))
                .createdAt(Instant.parse(createdAt))
                .build();
    }

    private void prepareTableData() throws Exception {
        String sqlFormat = "REPLACE into admin_event ("
                + "id, app_name, vendor, region, instance_id, directive, expiry_at, created_at) "
                + "values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
        String json1 ="{\"services\": {\"cmd\": \"stop\"}}";
        String sql1 = String.format(sqlFormat, "uuid1", "pbs", vendor, "east", "host1", json1,
                expiryAt, createdAt);
        connection.createStatement().execute(sql1);

        String expiryAt2= Instant.parse(expiryAt).plus(5, ChronoUnit.MINUTES).toString();
        String createdAt2 = Instant.parse(createdAt).plus(5, ChronoUnit.MINUTES).toString();
        String json2 ="{\"services\": {\"cmd\": \"start\"}}";
        String sql2 = String.format(sqlFormat, "uuid2", "pbs", vendor, "east", "host1", json2,
                expiryAt2, createdAt2);
        connection.createStatement().execute(sql2);
    }

}
