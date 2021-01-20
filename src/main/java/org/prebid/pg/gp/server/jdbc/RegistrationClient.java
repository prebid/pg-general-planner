package org.prebid.pg.gp.server.jdbc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.Registration;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A client to access database for PBS registration.
 */
public class RegistrationClient {

    private static final Logger logger = LoggerFactory.getLogger(RegistrationClient.class);

    private static final String FROM_REGISTRATION = "FROM app_registration ";

    private static final String FIND_ACTIVE_HOSTS_SQL =
            "SELECT instance_id, region, vendor, health_index, ad_reqs_per_sec, created_at "
            + FROM_REGISTRATION
            + "WHERE created_at > ? ";

    private static final String FIND_ACTIVE_HOST_SQL =
            "SELECT instance_id, region, vendor, health_index, ad_reqs_per_sec, created_at "
            + FROM_REGISTRATION
            + "WHERE instance_id = ? AND region = ? AND vendor = ? AND created_at > ? ";

    private static final String UPDATE_REGISTRATION_SQL =
            "REPLACE INTO app_registration "
            + "(app_name, instance_id, region, vendor, health_index, ad_reqs_per_sec, health_details) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?);";

    private static final String FIND_REGISTRATIONS_SQL =
            "SELECT instance_id, region, vendor, health_details, created_at "
            + FROM_REGISTRATION
            + "WHERE created_at > ? ";

    private final AdminTracer tracer;

    private final Metrics metrics;

    public RegistrationClient(Metrics metrics, AdminTracer adminTracer) {
        this.metrics = Objects.requireNonNull(metrics);
        this.tracer = Objects.requireNonNull(adminTracer);
    }

    Future<List<Map<String, Object>>> findRegistrations(
            SQLConnection sqlConnection, Instant activeSince, String vendor, String region, String instanceId) {
        Objects.requireNonNull(activeSince);
        final Future<ResultSet> future = Future.future();
        final long start = System.currentTimeMillis();

        StringBuilder sb = new StringBuilder(FIND_REGISTRATIONS_SQL);
        JsonArray params = new JsonArray().add(activeSince);
        buildFindRegistrationQuery(sb, params, "vendor", vendor);
        buildFindRegistrationQuery(sb, params, "region", region);
        buildFindRegistrationQuery(sb, params, "instance_id", instanceId);

        sqlConnection.queryWithParams(sb.toString(), params, ar -> {
            sqlConnection.close();
            final String method = "find-registrations";
            metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
            if (!ar.succeeded()) {
                logger.error("Error reading table app_registration::{0}", ar.cause().getMessage());
                metrics.incCounter(metricName(method + ".exc"));
            } else {
                logger.debug("findRegistrations::number of pbs apps::" + ar.result().getNumRows());
            }
            future.handle(ar);
        });
        return future.map(this::mapToRegistrations);
    }

    private void buildFindRegistrationQuery(StringBuilder sb, JsonArray params, String field, String val) {
        if (StringUtils.isEmpty(val)) {
            return;
        }
        sb.append(" AND ").append(field).append(" = ? ");
        params.add(val);
    }

    private List<Map<String, Object>> mapToRegistrations(ResultSet resultSet) {
        List<Map<String, Object>> registrations = new ArrayList<>();
        for (JsonArray row : resultSet.getResults()) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("instanceId", row.getString(0));
            map.put("region", row.getString(1));
            map.put("vendor", row.getString(2));
            map.put("status", decodeStatus(row.getString(3)));
            Instant createdAt = row.getInstant(4);
            String timestamp = createdAt == null ? null : createdAt.toString().replace("Z", ".000Z");
            map.put("latestRegistrationTimeStamp", timestamp);
            registrations.add(map);
        }
        return registrations;
    }

    private ObjectNode decodeStatus(String json) {
        return StringUtils.isEmpty(json) ? null : Json.decodeValue(json, ObjectNode.class);
    }

    Future<UpdateResult> updateRegistration(SQLConnection sqlConnection, Registration registration) {
        if (registration == null) {
            sqlConnection.close();
            return Future.failedFuture("Null Registration object");
        }

        final Future<UpdateResult> future = Future.future();

        final JsonArray params = new JsonArray()
                .add("PBS")
                .add(registration.getInstanceId())
                .add(registration.getRegion())
                .add(registration.getVendor())
                .add(registration.getHealthIndex())
                .add(registration.getAdReqsPerSec())
                .add(Json.encode(registration.getStatus()));

        final String method = "update-registration";
        final long start = System.currentTimeMillis();
        sqlConnection.updateWithParams(
                UPDATE_REGISTRATION_SQL,
                params,
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (ar.succeeded()) {
                        logger.debug("Persist PBS registration successfully");
                    } else {
                        logger.error("Failure in persisting PBS registration::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    future.handle(ar);
                });

        return future;
    }

    public Future<List<PbsHost>> findActiveHosts(SQLConnection sqlConnection, Instant activeSince) {
        final Future<ResultSet> future = Future.future();

        final String method = "find-active-hosts";
        final long start = System.currentTimeMillis();
        sqlConnection.queryWithParams(
                FIND_ACTIVE_HOSTS_SQL,
                new JsonArray().add(activeSince),
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logger.error("Error reading table app_registration::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    } else {
                        logger.debug("findActiveHosts::number of pbs apps::" + ar.result().getNumRows());
                    }
                    future.handle(ar);
            });

        return future.map(this::mapToPbsHostList);
    }

    Future<PbsHost> findActiveHost(SQLConnection sqlConnection, PbsHost pbsHost, Instant activeSince) {
        if (pbsHost == null) {
            sqlConnection.close();
            if (tracer.checkActive()) {
                logger.info("{0}::host NULL is NOT active since {1}", GPConstants.TRACER, activeSince.toString());
            }
            return Future.succeededFuture(PbsHost.builder().build());
        }

        if (tracer.checkActive()
                && tracer.matchVendor(pbsHost.getVendor()) && tracer.matchRegion(pbsHost.getRegion())) {
            logger.info("{0}::checking if host {1} is active since {2}",
                    GPConstants.TRACER, pbsHost, activeSince.toString());
        }

        final Future<ResultSet> future = Future.future();

        final JsonArray params = new JsonArray()
                .add(pbsHost.getHostInstanceId())
                .add(pbsHost.getRegion())
                .add(pbsHost.getVendor())
                .add(activeSince);
        long start = System.currentTimeMillis();

        final String method = "find-active-host";

        sqlConnection.queryWithParams(
                FIND_ACTIVE_HOST_SQL,
                params,
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                    if (!ar.succeeded()) {
                        logger.error("Failure in reading from table app_registration::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    } else {
                        if (tracer.checkActive()) {
                            logger.debug("{0}::host {1} is active since {2}",
                                    GPConstants.TRACER, pbsHost, activeSince.toString());
                        }
                    }
                    future.handle(ar);
                });

        return future.map(this::mapToPbsHost);
    }

    private List<PbsHost> mapToPbsHostList(ResultSet resultSet) {
        List<PbsHost> pbsHosts = new ArrayList<>();

        for (JsonArray row : resultSet.getResults()) {
            int index = 0;
            PbsHost pbsHost = PbsHost.builder()
                    .hostInstanceId(row.getString(index++))
                    .region(row.getString(index++))
                    .vendor(row.getString(index++))
                    .healthIndex(row.getFloat(index++))
                    .adReqsPerSec(row.getInteger(index++))
                    .createdAt(row.getInstant(index))
                    .build();
            pbsHosts.add(pbsHost);
            if (tracer.checkActive()
                    && tracer.matchRegion(pbsHost.getRegion())
                    && tracer.matchVendor(pbsHost.getVendor())) {
                logger.info("{0}::{1}", GPConstants.TRACER, pbsHost);
            }
        }
        return pbsHosts;
    }

    private PbsHost mapToPbsHost(ResultSet resultSet) {
        for (JsonArray row : resultSet.getResults()) {
            PbsHost pbsHost = PbsHost.builder()
                    .hostInstanceId(row.getString(0))
                    .region(row.getString(1))
                    .vendor(row.getString(2))
                    .build();
            if (tracer.checkActive()
                    && tracer.matchRegion(pbsHost.getRegion())
                    && tracer.matchVendor(pbsHost.getVendor())) {
                logger.info("{0}::{1}", GPConstants.TRACER, pbsHost);
            }
            return pbsHost;
        }

        return PbsHost.builder().build();
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }

}

