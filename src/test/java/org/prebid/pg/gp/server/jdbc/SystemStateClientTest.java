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
import org.prebid.pg.gp.server.model.SystemState;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@ExtendWith(VertxExtension.class)
class SystemStateClientTest {

    private static final String JDBC_URL = "jdbc:h2:mem:test";

    private static Vertx vertx;

    private static JDBCClient jdbcClient;

    private Connection connection;

    private SystemStateClient systemStateClient;

    @BeforeAll
    static void prepare() {
        vertx = Vertx.vertx();
        jdbcClient = jdbcClient();
    }

    @BeforeEach
    void setUpBeforeEach() throws Exception {
        systemStateClient = new SystemStateClient(new Metrics(new MetricRegistry()));
        connection = DriverManager.getConnection(JDBC_URL);

        connection.createStatement().execute(
                "CREATE TABLE system_state (tag varchar(128) NOT NULL, val varchar(128) NOT NULL,"
                    + " updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                    + " PRIMARY KEY (tag)); ");

        connection.createStatement().execute(
                "REPLACE INTO system_state (tag, val ) VALUES ('tag1', '2019-07-20 18:07:09.0');"
        );

        connection.createStatement().execute(
                "REPLACE INTO system_state (tag, val ) VALUES ('tag2', '2019-07-25 18:07:09.0');"
        );
    }

    @AfterAll
    static void cleanUp() {
        vertx.close();
    }

    @AfterEach
    void cleanUpAfterEach() throws Exception {
        try {
            connection.createStatement().execute("DROP TABLE system_state;");
        } catch (Exception e) {
            // do nothing
        } finally {
            connection.close();
        }
    }

    @Test
    void updateSystemStateWithUTCTime(VertxTestContext context) {
        final SystemState systemState = SystemState.builder().tag("last_time").val("2019-07-25T19:17:36.392Z").build();

        final Future<UpdateResult> future =
                connect().compose(sqlConnection -> systemStateClient.updateSystemStateWithUTCTime(sqlConnection, systemState));

        future.setHandler(context.succeeding(updateResult ->
            context.verify(() -> {
                assertThat(updateResult.getUpdated(), equalTo(1));
                context.completeNow();
            })
        ));
    }


    @Test
    void shouldReadUTCTimeValFromSystemState(VertxTestContext context) {
        final String expectedVal = "2019-07-20T18:07:09Z";
        final String tag = "tag1";

        final Future<String> valFuture =
                connect().compose(sqlConnection -> systemStateClient.readUTCTimeValFromSystemState(sqlConnection, tag));

        valFuture.setHandler(context.succeeding(actualVal ->
            context.verify(() -> {
                assertThat(actualVal, equalTo(expectedVal));
                context.completeNow();
            })
        ));
    }

    @Test
    void shouldReturnUTCTimeOf1970WhenTagIsNotFound(VertxTestContext context) {
        final String expectedVal = "1970-01-01T00:00:00Z";
        final String tag = "foo";

        final Future<String> valFuture =
                connect().compose(sqlConnection -> systemStateClient.readUTCTimeValFromSystemState(sqlConnection, tag));

        valFuture.setHandler(context.succeeding(actualVal ->
            context.verify(() -> {
                assertThat(actualVal, equalTo(expectedVal));
                context.completeNow();
            })
        ));
    }

    @Test
    void shouldReturnExceptionWhenThereAreDatabaseErrorsInReadingUTCTimeValFromSystemState(VertxTestContext context) throws Exception{
        connection.createStatement().execute("DROP TABLE system_state;");

        final Future<String> valFuture =
                connect().compose(sqlConnection -> systemStateClient.readUTCTimeValFromSystemState(sqlConnection, "foo"));

        valFuture.setHandler(context.failing(actual ->
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            })
        ));
    }

    @Test
    void shouldReturnEmptyObjectWhenQueryIsNullInReadingUTCTimeValFromSystemState(VertxTestContext context) {
        final Future<String> valFuture =
                connect().compose(sqlConnection -> systemStateClient.readUTCTimeValFromSystemState(sqlConnection, null));

        valFuture.setHandler(context.failing(actual ->
            context.verify(() -> {
                assertThat(actual.getMessage(), equalTo("Null tag to query"));
                context.completeNow();
            })
        ));
    }

    @Test
    void shouldReturnExceptionWhenThereAreDatabaseErrorsInUpdatingSystemStateWithUTCTime(VertxTestContext context) throws Exception{
        connection.createStatement().execute("DROP TABLE system_state;");

        final SystemState systemState = SystemState.builder().tag("last_time").val("2019-07-25T19:17:36.392Z").build();

        final Future<UpdateResult> future =
                connect().compose(sqlConnection -> systemStateClient.updateSystemStateWithUTCTime(sqlConnection, systemState));

        future.setHandler(context.failing(actual ->
            context.verify(() -> {
                assertThat(actual.getClass().getName(), equalTo("org.h2.jdbc.JdbcSQLException"));
                context.completeNow();
            })
        ));
    }

    @Test
    void shouldReturnFailedFutureWhenSystemStateObjectIsNullInUpdatingSystemStateWithUTCTime(VertxTestContext context) {
        final Future<UpdateResult> future =
                connect().compose(sqlConnection -> systemStateClient.updateSystemStateWithUTCTime(sqlConnection, null));

        future.setHandler(context.failing(actual ->
            context.verify(() -> {
                assertThat(actual.getMessage(), equalTo("Null SystemState object"));
                context.completeNow();
            })
        ));
    }

    private Future<SQLConnection> connect() {
        final Future<SQLConnection> future = Future.future();
        jdbcClient.getConnection(future);
        return future;
    }

    private static JDBCClient jdbcClient() {
        return JDBCClient.createShared(vertx,
                new JsonObject()
                        .put("url", JDBC_URL)
                        .put("driver_class", "org.h2.Driver")
                        .put("max_pool_size", 10));
    }
}