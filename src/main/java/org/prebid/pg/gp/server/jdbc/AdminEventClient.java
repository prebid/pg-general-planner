package org.prebid.pg.gp.server.jdbc;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AdminEvent.Directive;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.util.StringUtil;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A client to access database for {@link AdminEvent} information
 */
public class AdminEventClient {

    private static final Logger logger = LoggerFactory.getLogger(AdminEventClient.class);

    private static final String UPDATE_ADMIN_COMMAND_SQL =
            "REPLACE INTO admin_event "
            + "(id, app_name, vendor, region, instance_id, directive, expiry_at, created_at) "
            + "VALUES ";

    private static final String FIND_EARLIEST_ACTIVE_ADMIN_EVENT_SQL =
            "SELECT id, app_name, vendor, region, instance_id, directive, expiry_at, created_at "
            + "FROM admin_event "
            + "WHERE app_name = ? AND vendor = ? AND region = ? AND instance_id = ? AND expiry_at > ? "
            + "ORDER BY created_at ASC "
            + "LIMIT 1";

    private static final String DELETE_BY_ID_SQL = "DELETE FROM admin_event WHERE id = ?";

    private final Metrics metrics;

    public AdminEventClient(Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);
    }

    Future<UpdateResult> updateAdminEvents(SQLConnection sqlConnection, List<AdminEvent> entities) {
        final Future<UpdateResult> future = Future.future();
        StringBuilder sb = StringUtil.appendRepeatedly(new StringBuilder(UPDATE_ADMIN_COMMAND_SQL),
                "(?, ?, ?, ?, ?, ?, ?, ?)", ",", entities.size());
        final JsonArray params = new JsonArray();
        for (AdminEvent entity : entities) {
            params.add(entity.getId())
                    .add(entity.getApp())
                    .add(entity.getVendor())
                    .add(entity.getRegion())
                    .add(entity.getInstanceId())
                    .add(Json.encode(entity.getDirective()))
                    .add(entity.getExpiryAt())
                    .add(entity.getCreatedAt());
        }

        final String method = "update-admin-events";
        final long start = System.currentTimeMillis();
        sqlConnection.updateWithParams(
                sb.toString(),
                params,
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (ar.succeeded()) {
                        logger.debug("Persist admin events successfully");
                    } else {
                        logger.error("Failure in persisting admin events::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    future.handle(ar);
                });
        return future;
    }

    Future<AdminEvent> findEarliestActiveAdminEvent(
            SQLConnection sqlConnection, String app, Registration registration, Instant expiryAt) {
        final Future<ResultSet> future = Future.future();
        final long start = System.currentTimeMillis();

        JsonArray params = new JsonArray()
                .add(app)
                .add(registration.getVendor())
                .add(registration.getRegion())
                .add(registration.getInstanceId())
                .add(expiryAt);

        sqlConnection.queryWithParams(FIND_EARLIEST_ACTIVE_ADMIN_EVENT_SQL,
                params,
                ar -> {
                    sqlConnection.close();
                    final String method = "find-earliest-active-admin-event";
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logger.error("Error reading table admin_event::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    } else {
                        logger.debug("findEarliestActiveAdminEvent::" + ar.result().getResults());
                    }
                    future.handle(ar);
                });
        return future.map(this::mapToAdminEvent);
    }

    private AdminEvent mapToAdminEvent(ResultSet resultSet) {
        if (resultSet.getResults().isEmpty()) {
            return null;
        }
        JsonArray arr = resultSet.getResults().get(0);
        return AdminEvent.builder()
                .id(arr.getString(0))
                .app(arr.getString(1))
                .vendor(arr.getString(2))
                .region(arr.getString(3))
                .instanceId(arr.getString(4))
                .directive(Json.decodeValue(arr.getString(5), Directive.class))
                .expiryAt(arr.getInstant(6))
                .createdAt(arr.getInstant(7))
                .build();
    }

    public Future<UpdateResult> deleteAdminEvent(SQLConnection sqlConnection, String id) {
        final Future<UpdateResult> future = Future.future();
        JsonArray params = new JsonArray().add(id);

        final String method = "delete-admin-event";
        final long start = System.currentTimeMillis();
        sqlConnection.updateWithParams(DELETE_BY_ID_SQL, params, ar -> {
            sqlConnection.close();
            metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
            if (!ar.succeeded()) {
                logger.error("Error delete admin_event::{0}", ar.cause().getMessage());
                metrics.incCounter(metricName(method + ".exc"));
            } else {
                logger.info("deleteAdminEvent::" + id);
            }
            future.handle(ar);
        });
        return future;
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }

}
