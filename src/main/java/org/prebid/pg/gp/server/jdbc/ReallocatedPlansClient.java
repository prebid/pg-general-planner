package org.prebid.pg.gp.server.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.ResultSetType;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import org.prebid.pg.gp.server.exception.GeneralPlannerException;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.ReallocationWeights;
import org.prebid.pg.gp.server.model.Weightage;
import org.prebid.pg.gp.server.util.Validators;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A client to access database for reallocated line item delivery plans.
 */
public class ReallocatedPlansClient {

    private static final Logger logger = LoggerFactory.getLogger(ReallocatedPlansClient.class);

    private final String generalPlannerHostInstanceId;

    private static final String GET_REALLOCATED_PLANS_SQL =
            "SELECT instance_id, region, vendor, token_reallocation_weights, updated_at "
            + "FROM reallocated_plans "
            + "WHERE service_instance_id = ? AND instance_id = ? AND region = ? AND vendor = ?";

    private static final String GET_RECENTLY_UPDATED_REALLOCATED_PLANS_SQL =
            "SELECT instance_id, region, vendor, token_reallocation_weights, updated_at "
            + "FROM reallocated_plans "
            + "WHERE service_instance_id = ? AND updated_at >= ?";

    private static final String UPDATE_REALLOCATED_PLANS_SQL =
            "REPLACE INTO reallocated_plans "
            + "(service_instance_id, vendor, region, instance_id, token_reallocation_weights, updated_at) "
            + "VALUES ( ?, ?, ?, ?, ?, ?)";

    private final AdminTracer tracer;

    private final Metrics metrics;

    public ReallocatedPlansClient(String hostInstanceId, Metrics metrics, AdminTracer adminTracer) {
        this.generalPlannerHostInstanceId = Validators.checkArgument(
                hostInstanceId, !StringUtils.isEmpty(hostInstanceId), "hostInstanceId should not be blank.");
        this.metrics = Objects.requireNonNull(metrics);
        this.tracer = Objects.requireNonNull(adminTracer);
    }

    Future<List<ReallocatedPlan>> getLatestReallocatedPlans(SQLConnection sqlConnection, Instant updatedSince) {
        final long start = System.currentTimeMillis();
        final Future<ResultSet> resultSetFuture = Future.future();
        final String method = "read-recently-updated_reallocated-plans";
        JsonArray params = new JsonArray().add(generalPlannerHostInstanceId).add(updatedSince);

        sqlConnection.setOptions(new SQLOptions().setResultSetType(ResultSetType.FORWARD_ONLY))
                .queryWithParams(
                        GET_RECENTLY_UPDATED_REALLOCATED_PLANS_SQL,
                        params,
                        ar -> {
                            sqlConnection.close();
                            metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                            if (!ar.succeeded()) {
                                logger.error(
                                        "Failure in reading reallocated_plans information for {0}::{1}::{2} => {3}",
                                        generalPlannerHostInstanceId, updatedSince, ar.cause().getMessage()
                                );
                                metrics.incCounter(metricName(method + ".exc"));
                            }
                            resultSetFuture.handle(ar);
                        });

        return resultSetFuture.map(this::mapToReallocatedPlansBatch);
    }

    Future<ReallocatedPlan> getReallocatedPlan(
            SQLConnection sqlConnection, String hostInstanceId, String region, String vendor) {
        final long start = System.currentTimeMillis();
        final Future<ResultSet> resultSetFuture = Future.future();

        if (hostInstanceId == null || region == null || vendor == null) {
            sqlConnection.close();
            return Future.succeededFuture(ReallocatedPlan.builder().build());
        }

        JsonArray params = new JsonArray()
                .add(generalPlannerHostInstanceId)
                .add(hostInstanceId)
                .add(region)
                .add(vendor);
        final String method = "read-reallocated-plans";

        sqlConnection.setOptions(new SQLOptions().setResultSetType(ResultSetType.FORWARD_ONLY))
                .queryWithParams(
                    GET_REALLOCATED_PLANS_SQL,
                    params,
                    ar -> {
                        sqlConnection.close();
                        metrics.updateTimer(metricName(method), System.currentTimeMillis() - start);
                        if (!ar.succeeded()) {
                            logger.error(
                                    "Failure in reading reallocated_plans information for {0}::{1}::{2} => {3}",
                                    generalPlannerHostInstanceId, hostInstanceId, region, ar.cause().getMessage()
                            );
                            metrics.incCounter(metricName(method + ".exc"));
                        }
                        resultSetFuture.handle(ar);
                    });

        return resultSetFuture.map(this::mapToReallocatedPlans);
    }

    private List<ReallocatedPlan> mapToReallocatedPlansBatch(ResultSet resultSet) {
        List<ReallocatedPlan> reallocatedPlans = new ArrayList<>();
        if (resultSet.getResults() != null) {
            for (JsonArray row : resultSet.getResults()) {
                try {
                    ReallocatedPlan reallocatedPlan = ReallocatedPlan.builder()
                            .instanceId(row.getString(0))
                            .region(row.getString(1))
                            .vendor(row.getString(2))
                            .reallocationWeights(
                                    new ObjectMapper().readValue(row.getString(3), ReallocationWeights.class))
                            .updatedAt(row.getInstant(4))
                            .build();
                    reallocatedPlans.add(reallocatedPlan);
                    if (tracer.checkActive()) {
                        for (Weightage weightage : reallocatedPlan.getReallocationWeights().getWeights()) {
                            if (tracer.matchVendor(reallocatedPlan.getVendor())
                                    && tracer.matchRegion(reallocatedPlan.getRegion())
                                    && tracer.matchBidderCode(weightage.getBidderCode())
                                    && tracer.matchLineItemId(weightage.getLineItemId())) {
                                logger.info(
                                        "{0}::{1}|{2}|{3}::{4}",
                                        GPConstants.TRACER,
                                        reallocatedPlan.getVendor(), reallocatedPlan.getRegion(),
                                        reallocatedPlan.getInstanceId(), weightage.toString());
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Exception in creating ReallocatedPlan object::{0}", e.getMessage());
                    metrics.incCounter(metricName(".json.exc"));
                    throw new GeneralPlannerException(e);
                }
            }
        }
        return reallocatedPlans;
    }

    private ReallocatedPlan mapToReallocatedPlans(ResultSet resultSet) {
        ReallocatedPlan reallocatedPlan = null;
        if (resultSet.getResults() != null && resultSet.getResults().size() == 1) {
            JsonArray row = resultSet.getResults().get(0);
            try {
                reallocatedPlan = ReallocatedPlan.builder()
                        .instanceId(row.getString(0))
                        .region(row.getString(1))
                        .vendor(row.getString(2))
                        .reallocationWeights(
                                new ObjectMapper().readValue(row.getString(3), ReallocationWeights.class))
                        .updatedAt(row.getInstant(4))
                        .build();

                if (tracer.checkActive()) {
                    for (Weightage weightage : reallocatedPlan.getReallocationWeights().getWeights()) {
                        if (tracer.matchVendor(reallocatedPlan.getVendor())
                                && tracer.matchRegion(reallocatedPlan.getRegion())
                                && tracer.matchBidderCode(weightage.getBidderCode())
                                && tracer.matchLineItemId(weightage.getLineItemId())) {
                            logger.info(
                                    "{0}::{1}|{2}|{3}::{4}",
                                    GPConstants.TRACER,
                                    reallocatedPlan.getVendor(), reallocatedPlan.getRegion(),
                                    reallocatedPlan.getInstanceId(), weightage.toString());
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Exception in creating ReallocatedPlan object::{0}", e.getMessage());
                metrics.incCounter(metricName(".json.exc"));
                throw new GeneralPlannerException(e);
            }
        }
        return reallocatedPlan;
    }

    Future<List<Integer>> updateReallocatedPlans(SQLConnection sqlConnection, List<ReallocatedPlan> reallocatedPlans) {
        final long start = System.currentTimeMillis();

        reallocatedPlans.stream().peek(logger::debug);
        logger.debug(UPDATE_REALLOCATED_PLANS_SQL);

        final Future<List<Integer>> updateResultFuture = Future.future();

        List<JsonArray> params = reallocatedPlans.stream()
                .map(reallocatedPlan -> new JsonArray()
                        .add(this.generalPlannerHostInstanceId)
                        .add(reallocatedPlan.getVendor())
                        .add(reallocatedPlan.getRegion())
                        .add(reallocatedPlan.getInstanceId())
                        .add(Json.encode(reallocatedPlan.getReallocationWeights()))
                        .add(reallocatedPlan.getUpdatedAt()))
                .collect(Collectors.toList());

        final String method = "update-reallocated-plans";
        sqlConnection.batchWithParams(UPDATE_REALLOCATED_PLANS_SQL,
                params,
                ar -> {
                    sqlConnection.close();
                    metrics.updateTimer(method, System.currentTimeMillis() - start);
                    if (ar.succeeded()) {
                        logger.debug("Persist reallocated plan successfully");
                        logger.info(
                                "Batch updateReallocatedPlans processed in {0}ms",
                                System.currentTimeMillis() - start
                        );
                    } else {
                        logger.error("Failure in persisting reallocated plan weights::{0}", ar.cause().getMessage());
                        metrics.incCounter(metricName(method + ".exc"));
                    }
                    updateResultFuture.handle(ar);
                });

        if (tracer.checkActive()) {
            for (ReallocatedPlan reallocatedPlan : reallocatedPlans) {
                for (Weightage weightage : reallocatedPlan.getReallocationWeights().getWeights()) {
                    if (tracer.matchVendor(reallocatedPlan.getVendor())
                            && tracer.matchRegion(reallocatedPlan.getRegion())
                            && tracer.matchBidderCode(weightage.getBidderCode())
                            && tracer.matchLineItemId(weightage.getLineItemId())) {
                        logger.info(
                                "{0}::Update::{1}|{2}|{3}::{4}",
                                GPConstants.TRACER,
                                reallocatedPlan.getVendor(), reallocatedPlan.getRegion(),
                                reallocatedPlan.getInstanceId(), weightage.toString()
                        );
                    }
                }
            }
        }
        return updateResultFuture;
    }

    private String metricName(String tag) {
        return String.format("db-access.%s", tag);
    }
}
