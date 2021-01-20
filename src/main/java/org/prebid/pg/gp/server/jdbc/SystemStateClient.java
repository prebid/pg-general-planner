package org.prebid.pg.gp.server.jdbc;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.SystemState;

import java.time.Instant;
import java.util.Objects;

/**
 * A client to access database for system state information.
 */
public class SystemStateClient {

    private static final Logger logger = LoggerFactory.getLogger(SystemStateClient.class);

    private static final String UPDATE_SYSTEM_STATE_SQL =
            "REPLACE INTO system_state (tag, val) VALUES (?, ?);";

    private final Metrics metrics;

    public SystemStateClient(Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);
    }

    public Future<UpdateResult> updateSystemStateWithUTCTime(SQLConnection sqlConnection, SystemState systemState) {
        if (systemState == null) {
            sqlConnection.close();
            return Future.failedFuture("Null SystemState object");
        }

        final Future<UpdateResult> updateResultFuture = Future.future();

        final JsonArray params = new JsonArray()
                .add(systemState.getTag())
                .add(systemState.getVal().substring(0, 19).concat("Z"));
        final long start = System.currentTimeMillis();

        final String method = "update-system-state";
        sqlConnection.updateWithParams(
                UPDATE_SYSTEM_STATE_SQL,
                params,
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (ar.succeeded()) {
                        logger.debug("Updated system_state information with {0}", systemState);
                    } else {
                        logger.error("Failure in updating system_state information::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    updateResultFuture.handle(ar);
                });

        return updateResultFuture;
    }

    public Future<String> readUTCTimeValFromSystemState(SQLConnection sqlConnection, String tag) {
        if (tag == null) {
            sqlConnection.close();
            return Future.failedFuture("Null tag to query");
        }

        final String method = "read-system-state";
        final Future<ResultSet> resultSetFuture = Future.future();
        final String readSql = "select val from system_state where tag = ?";
        final long start = System.currentTimeMillis();
        sqlConnection.queryWithParams(readSql, new JsonArray().add(tag), ar -> {
            sqlConnection.close();
            metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
            if (!ar.succeeded()) {
                logger.error(
                        "Failure in reading system_state time information for tag {0}::{1}",
                        tag, ar.cause().getMessage()
                );
                metrics.incCounter(metricName(method + ".exc"));
            }
            resultSetFuture.handle(ar);
        });

        return resultSetFuture.map(rs -> mapToUTCTimestampString(tag, rs));
    }

    private String mapToUTCTimestampString(String tag, ResultSet resultSet) {
        String val;

        if (resultSet.getResults() == null || resultSet.getResults().isEmpty()) {
            val = Instant.EPOCH.toString();
        } else {
            val = resultSet.getResults().get(0).getString(0).replace(" ", "T").replace(".0", "Z");
        }
        logger.info("System state entry::{0}::{1}", tag, val);

        return val;
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }
}
