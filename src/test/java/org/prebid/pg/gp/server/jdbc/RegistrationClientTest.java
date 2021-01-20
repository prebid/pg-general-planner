package org.prebid.pg.gp.server.jdbc;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.model.TracerFilters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsEqual.equalTo;

@ExtendWith(VertxExtension.class)
class RegistrationClientTest {

    private static final String JDBC_URL = "jdbc:h2:mem:test";

    private static Vertx vertx;

    private static JDBCClient jdbcClient;

    private Connection connection;

    private RegistrationClient registrationClient;

    private AdminTracer tracer;

    private String vendor = "vendor1";

    @BeforeAll
    static void prepare() {
        vertx = Vertx.vertx();
        jdbcClient = jdbcClient();
    }

    @BeforeEach
    void setUpBeforeEach() throws Exception {
        tracer = new AdminTracer();
        TracerFilters filters = new TracerFilters();
        filters.setRegion("us-west");
        filters.setVendor(vendor);
        tracer.setEnabled(true);
        tracer.setExpiresAt(Instant.now().plusSeconds(86400));
        tracer.setFilters(filters);
        registrationClient = new RegistrationClient(new Metrics(new MetricRegistry()), tracer);

        connection = DriverManager.getConnection(JDBC_URL);

        connection.createStatement().execute(
                "CREATE TABLE `app_registration` ("
                + "  `app_name` varchar(64) NOT NULL,"
                + "  `vendor` varchar(64) NOT NULL,"
                + "  `region` varchar(32) NOT NULL,"
                + "  `instance_id` varchar(64) NOT NULL,"
                + "  `health_index` decimal(2,1) NOT NULL,"
                + "  `ad_reqs_per_sec` mediumint(9),"
                + "  `health_details` clob,"
                + "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "  PRIMARY KEY (`app_name`, `vendor`, `region`, `instance_id`))");

        connection.createStatement().execute("REPLACE INTO app_registration ("
                + "app_name, instance_id, region, vendor, health_index, ad_reqs_per_sec"
                + ") VALUES ('PBS', 'host-1', 'us-east', 'vendor1', 0.9, 110);");

        connection.createStatement().execute("REPLACE INTO app_registration ("
                + "app_name, instance_id, region, vendor, health_index, ad_reqs_per_sec"
                + ") VALUES ('PBS', 'host-2', 'us-east', 'vendor1', 0.9, 110);");

        connection.createStatement().execute("REPLACE INTO app_registration ("
                + "app_name, instance_id, region, vendor, health_index, ad_reqs_per_sec"
                + ") VALUES ('PBS', 'host-1', 'us-west', 'vendor1', 0.9, 110);");
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        vertx.close();
    }

    @AfterEach
    void cleanUpAfterEach() throws Exception {
        try {
            connection.createStatement().execute("DROP TABLE app_registration;");
        } catch (Exception e) {
            // do nothing
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldUpdateRegistration(VertxTestContext context) {
        final Registration registration = Registration.builder()
                .region("hk").instanceId("m1").vendor("foo").healthIndex(.7f)
                .adReqsPerSec(88).build();

        final Future<UpdateResult> future = connect().compose(
                sqlConnection -> registrationClient.updateRegistration(sqlConnection, registration));

        future.setHandler(context.succeeding(updateResult -> {
            context.verify(() -> {
                assertThat(updateResult.getUpdated(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldUpdateRegistrationWhenAdReqPerSecMissing(VertxTestContext context) {
        final Registration registration = Registration.builder()
                .region("hk").instanceId("m1").vendor("foo").healthIndex(.7f)
                .build();

        final Future<UpdateResult> future = connect().compose(
                sqlConnection -> registrationClient.updateRegistration(sqlConnection, registration));

        future.setHandler(context.succeeding(updateResult -> {
            context.verify(() -> {
                assertThat(updateResult.getUpdated(), equalTo(1));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionWhenThereAreDatabaseErrorsInUpdatingRegistration(VertxTestContext context)
            throws Exception {
        connection.createStatement().execute("DROP TABLE app_registration;");

        final Registration registration =
                Registration.builder().region("hk").instanceId("m1").healthIndex(.7f).adReqsPerSec(88).build();

        final Future<UpdateResult> future =
                connect().compose(sqlConnection -> registrationClient.updateRegistration(sqlConnection, registration));

        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionWhenRegistrationInformationIsIncompleteInUpdatingRegistration(VertxTestContext context)
            throws Exception{
        final Registration registration = Registration.builder().region("hk").build();

        final Future<UpdateResult> future =
                connect().compose(sqlConnection -> registrationClient.updateRegistration(sqlConnection, registration));

        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnFailedFutureWhenRegistrationObjectIsNullInUpdatingRegistration(VertxTestContext context) {
        final Future<UpdateResult> future =
                connect().compose(sqlConnection -> registrationClient.updateRegistration(sqlConnection, null));

        future.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getMessage(), equalTo("Null Registration object"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldFindThreeActiveHosts(VertxTestContext context) {
        final Set<String> expectedSet = new HashSet<String>();
        expectedSet.add(String.format("%s;%s", "us-west", "host-1"));
        expectedSet.add(String.format("%s;%s", "us-east", "host-1"));
        expectedSet.add(String.format("%s;%s", "us-east", "host-2"));

        final Future<List<PbsHost>> pbsHostListFuture =
                connect().compose(sqlConnection -> registrationClient.findActiveHosts(sqlConnection, since()));

        pbsHostListFuture.setHandler(context.succeeding(pbsHostList -> {
            context.verify(() -> {
                HashSet<String> actualSet = pbsHostList.stream()
                        .map(pbsHost -> String.format("%s;%s", pbsHost.getRegion(), pbsHost.getHostInstanceId()))
                        .collect(Collectors.toCollection(HashSet::new));
                assertThat(actualSet, equalTo(expectedSet));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldFindThreeActiveHostsCreatedLatest(VertxTestContext context) throws Exception {
        // this one should not be find
        connection.createStatement().execute("REPLACE INTO app_registration ("
                + "app_name, instance_id, region, vendor, health_index, ad_reqs_per_sec) "
                + "VALUES ('PBS', 'host-1', 'us-east', 'vendor1', 0.9, 10);");

        final Set<String> expectedSet = new HashSet<String>();
        expectedSet.add(String.format("%s;%s", "us-west", "host-1"));
        expectedSet.add(String.format("%s;%s", "us-east", "host-1"));
        expectedSet.add(String.format("%s;%s", "us-east", "host-2"));

        final Future<List<PbsHost>> pbsHostListFuture =
                connect().compose(sqlConnection -> registrationClient.findActiveHosts(sqlConnection, since()));

        pbsHostListFuture.setHandler(context.succeeding(pbsHostList -> {
            context.verify(() -> {
                HashSet<String> actualSet = pbsHostList.stream()
                        .map(pbsHost -> String.format("%s;%s", pbsHost.getRegion(), pbsHost.getHostInstanceId()))
                        .collect(Collectors.toCollection(HashSet::new));
                assertThat(actualSet, equalTo(expectedSet));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionWhenThereAreDatabaseErrorsInFindActiveHosts(VertxTestContext context) throws Exception{
        connection.createStatement().execute("DROP TABLE app_registration;");

        final Future<List<PbsHost>> pbsHostListFuture =
                connect().compose(sqlConnection -> registrationClient.findActiveHosts(sqlConnection, since()));

        pbsHostListFuture.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnExceptionWhenThereAreDatabaseErrorsInFindActiveHost(VertxTestContext context) throws Exception{
        connection.createStatement().execute("DROP TABLE app_registration;");

        final PbsHost queryPbsHost = PbsHost.builder().region("us-west").hostInstanceId("host-1").build();

        final Future<PbsHost> pbsHostFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, queryPbsHost, since()));

        pbsHostFuture.setHandler(context.failing(actual -> {
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnEmptyListWhereAreNoActiveHosts(VertxTestContext context) throws Exception{
        connection.createStatement().execute("TRUNCATE TABLE app_registration;");

        final ArrayList<PbsHost> expected = new ArrayList<>();

        final Future<List<PbsHost>> pbsHostListFuture =
                connect().compose(sqlConnection -> registrationClient.findActiveHosts(sqlConnection, since()));

        pbsHostListFuture.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual, samePropertyValuesAs(expected));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldFindTheActiveHost(VertxTestContext context) {
        final Set<String> expectedSet = new HashSet<String>();
        expectedSet.add(String.format("%s;%s;%s", vendor, "us-west", "host-1"));

        final HashSet<String> actualSet = new HashSet<>();

        final PbsHost queryPbsHost = PbsHost.builder()
                .region("us-west").hostInstanceId("host-1").vendor(vendor).build();
        final Future<PbsHost> pbsHostListFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, queryPbsHost, since()));

        pbsHostListFuture.setHandler(context.succeeding(pbsHost -> {
            context.verify(() -> {
                actualSet.add(String.format("%s;%s;%s",
                        pbsHost.getVendor(), pbsHost.getRegion(), pbsHost.getHostInstanceId()));
                assertThat(actualSet, equalTo(expectedSet));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnEmptyObjectWhenHostIsNotFoundInFindActiveHost(VertxTestContext context) {
        final PbsHost queryPbsHost = PbsHost.builder().region("us-west").hostInstanceId("host-1-x").build();
        final Future<PbsHost> pbsHostListFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, queryPbsHost, since()));

        pbsHostListFuture.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.getHostInstanceId(), is(nullValue()));
                assertThat(actual.getRegion(), is(nullValue()));
                assertThat(actual.getVendor(), is(nullValue()));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnEmptyObjectWhenHostIsNotFilledInFindActiveHost(VertxTestContext context) {
        final PbsHost queryPbsHost = PbsHost.builder().region("us-west").build();
        final Future<PbsHost> pbsHostListFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, queryPbsHost, since()));

        pbsHostListFuture.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.getHostInstanceId(), is(nullValue()));
                assertThat(actual.getRegion(), is(nullValue()));
                assertThat(actual.getVendor(), is(nullValue()));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnEmptyObjectWhenRegionIsNotFilledInFindActiveHost(VertxTestContext context) {
        final PbsHost queryPbsHost = PbsHost.builder().hostInstanceId("host-1").build();
        final Future<PbsHost> pbsHostListFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, queryPbsHost, since()));

        pbsHostListFuture.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.getHostInstanceId(), is(nullValue()));
                assertThat(actual.getRegion(), is(nullValue()));
                assertThat(actual.getVendor(), is(nullValue()));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnEmptyObjectWhenQueryIsNotFilledInFindActiveHost(VertxTestContext context) {
        final PbsHost queryPbsHost = PbsHost.builder().build();
        final Future<PbsHost> pbsHostListFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, queryPbsHost, since()));

        pbsHostListFuture.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.getHostInstanceId(), is(nullValue()));
                assertThat(actual.getRegion(), is(nullValue()));
                assertThat(actual.getVendor(), is(nullValue()));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldReturnEmptyObjectWhenQueryIsNullFindActiveHost(VertxTestContext context) {
        final Future<PbsHost> pbsHostListFuture = connect().compose(
                sqlConnection -> registrationClient.findActiveHost(sqlConnection, null, since()));

        pbsHostListFuture.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.getHostInstanceId(), is(nullValue()));
                assertThat(actual.getRegion(), is(nullValue()));
                assertThat(actual.getVendor(), is(nullValue()));
                context.completeNow();
            });
        }));
    }

    @Test
    void shouldFindRegistrations(VertxTestContext context) {
        final Future<List<Map<String, Object>>> future = connect().compose(
                sqlConnection -> registrationClient.findRegistrations(
                        sqlConnection, since(),vendor, "us-east", "host-1"));

        future.setHandler(context.succeeding(actual -> {
            context.verify(() -> {
                assertThat(actual.size(), equalTo(1));
                assertThat(actual.get(0).get("instanceId"), equalTo("host-1"));
                assertThat(actual.get(0).get("region"), equalTo("us-east"));
                assertThat(actual.get(0).get("vendor"), equalTo(vendor));
                context.completeNow();
            });
        }));
    }

    private Future<SQLConnection> connect() {
        final Future<SQLConnection> future = Future.future();
        jdbcClient.getConnection(future);
        return future;
    }

    private Future<SQLConnection> connectX() {
        return Future.succeededFuture(null);
    }

    private static JDBCClient jdbcClient() {
        return JDBCClient.createShared(vertx,
                new JsonObject()
                        .put("url", JDBC_URL)
                        .put("driver_class", "org.h2.Driver")
                        .put("max_pool_size", 10));
    }

    private Instant since() {
        return Instant.now().minusSeconds(180);
    }
}