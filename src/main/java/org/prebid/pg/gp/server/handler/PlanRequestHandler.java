package org.prebid.pg.gp.server.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.prebid.pg.gp.server.exception.InvalidRequestException;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.PlanRequest;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.Weightage;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;
import org.prebid.pg.gp.server.util.Constants;
import org.prebid.pg.gp.server.util.JsonUtil;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * A handler for retrieval of line item delivery plans request.
 */
public class PlanRequestHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(PlanRequestHandler.class);

    private static final String PG_SIM_TIMESTAMP_HEADER = "pg-sim-timestamp";

    static final String PLAN_REQUEST_KEY = "plan";

    private final String maskedErrorMessage;

    private final CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient;

    private final String resourceRole;

    private final boolean securityEnabled;

    private final HostReallocationConfiguration reallocationConfig;

    private final boolean isAlgoTest;

    private final AdminTracer tracer;

    private final Shutdown shutdown;

    private final Metrics metrics;

    private final AlertProxyHttpClient alertHttpClient;

    private final Random random;

    private final int pbsMaxIdlePeriodInSeconds;

    public PlanRequestHandler(
            CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredPlannerDataAccessClient,
            String maskedErrorMessage, String resourceRole, boolean securityEnabled,
            HostReallocationConfiguration reallocationConfig,
            int pbsMaxIdlePeriodInSeconds,
            Metrics metrics, boolean isAlgoTest,
            AdminTracer adminTracer, Shutdown shutdown,
            AlertProxyHttpClient alertHttpClient,
            Random random
    ) {
        this.dataAccessClient = circuitBreakerSecuredPlannerDataAccessClient;
        this.maskedErrorMessage = maskedErrorMessage;
        this.resourceRole = resourceRole;
        this.securityEnabled = securityEnabled;
        this.reallocationConfig = reallocationConfig;
        this.pbsMaxIdlePeriodInSeconds = pbsMaxIdlePeriodInSeconds;
        this.metrics = metrics;
        this.isAlgoTest = isAlgoTest;
        this.tracer = adminTracer;
        this.shutdown = shutdown;
        this.alertHttpClient = alertHttpClient;
        this.random = random;
        if (securityEnabled && !isAlgoTest) {
            logger.info("PlanRequestHandler protected by role {0}", resourceRole);
        }
    }

    /**
     * Handles retrieval of line items delivery plans request.
     *
     * @param routingContext context of request and response
     */
    @Override
    public void handle(RoutingContext routingContext) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            routingContext.response()
                    .setStatusCode(HttpResponseStatus.BAD_GATEWAY.code())
                    .end("Server shutdown has been initiated");
            return;
        }

        if (securityEnabled && !isAlgoTest) {
            routingContext.user().isAuthorized(resourceRole, rs -> {
                if (rs.succeeded() && rs.result().booleanValue()) {
                    processRequest(routingContext);
                } else {
                    HttpServerResponse response = routingContext.response();
                    response.setStatusCode(HttpResponseStatus.FORBIDDEN.code()).end();
                }
            });
        } else {
            processRequest(routingContext);
        }
    }

    private void processRequest(RoutingContext routingContext) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            routingContext.response()
                    .setStatusCode(HttpResponseStatus.BAD_GATEWAY.code())
                    .end("Server shutdown has been initiated");
            return;
        }

        String simTime = routingContext.request().getHeader(PG_SIM_TIMESTAMP_HEADER);
        if (isAlgoTest) {
            logger.info("Received sim time: {0}.", simTime);
        }
        Instant now = StringUtils.isEmpty(simTime) ? Instant.now() : Instant.parse(simTime);

        final long start = System.currentTimeMillis();
        final Instant hostActiveSince = Instant.now().minusSeconds(pbsMaxIdlePeriodInSeconds);
        parseRequest(routingContext)
                .compose(planRequest -> dataAccessClient.findActiveHost(planRequest, hostActiveSince))
                .compose(activeHost -> {
                    logger.debug("findActiveHost::{0}", activeHost);
                    return dataAccessClient.getReallocatedPlan(activeHost);
                })
                .compose(reallocatedPlan -> {
                    logger.debug("getReallocatedPlans::{0}", reallocatedPlan);
                    return getLineItems(now).map(lineItems -> new Pair<>(reallocatedPlan, lineItems));
                })
                .compose(pair -> {
                    logger.debug("findActiveHosts");
                    return dataAccessClient.findActiveHosts(hostActiveSince)
                            .map(activeHosts -> new Triplet<>(pair.getValue0(), pair.getValue1(), activeHosts));
                })
                .map(triplet -> {
                    logger.debug("getUpdatedPlan");
                    PlanRequest planRequest = routingContext.get(PLAN_REQUEST_KEY);
                    List<ObjectNode> lis = getUpdatedPlan(triplet, planRequest);
                    logger.info("Returning {0} line items with reallocated tokens to <{1}>|<{2}>|<{3}>",
                            lis.size(), planRequest.getVendor(), planRequest.getRegion(), planRequest.getInstanceId());
                    if (lis.isEmpty()) {
                        String msg = String.format("Empty plans for request:: %s",
                                routingContext.get(PLAN_REQUEST_KEY).toString());
                        logger.warn(msg);
                    }
                    return lis;
                })
                .setHandler(ar -> finalHandler(ar, routingContext, start));
    }

    private Future<PlanRequest> parseRequest(RoutingContext routingContext) {
        final MultiMap params = routingContext.request().params();
        if (params == null) {
            String code = GPConstants.BAD_FORMAT;
            String msg = "Cannot decode incoming plan request::missing required parameters::"
                    +
                    "'vendor,'region','instanceId'";
            logger.error("{0}::{1}", code, msg);
            return Future.failedFuture(new InvalidRequestException(code, msg));
        }

        List<String> vendors = params.getAll("vendor");
        List<String> regions = params.getAll("region");
        List<String> instances = params.getAll("instanceId");

        List<String> errors = validateQueryParams(vendors, regions, instances);
        if (!errors.isEmpty()) {
            String code = GPConstants.BAD_FORMAT;
            String msg = "Cannot decode incoming plan request::" + errors.toString();
            logger.error(code + "::" + msg);
            return Future.failedFuture(new InvalidRequestException(code, msg));
        }

        PlanRequest planRequest = PlanRequest.builder()
                .vendor(vendors.get(0))
                .region(regions.get(0))
                .instanceId(instances.get(0)).build();

        logger.debug("PBS_GetPlans_Request: {0}", planRequest.toString());

        routingContext.put(PLAN_REQUEST_KEY, planRequest);

        return Future.succeededFuture(planRequest);
    }

    private List<String> validateQueryParams(List<String> vendors, List<String> regions, List<String> instances) {
        List<String> errors = new ArrayList<>();

        if (vendors.isEmpty() || vendors.get(0).isEmpty()) {
            errors.add("'vendor' query parameter is required and must have a value");
        }
        if (regions.isEmpty() || regions.get(0).isEmpty()) {
            errors.add("'region' query parameter is required and must have a value");
        }
        if (instances.isEmpty() || instances.get(0).isEmpty()) {
            errors.add("'instanceId' query parameter is required and must have a value");
        }
        return errors;
    }

    private Future<List<LineItem>> getLineItems(Instant now) {
        Instant curTime = now == null ? Instant.now() : now;
        Instant inactiveSince = curTime.minus(reallocationConfig.getLineItemHasExpiredMin(), ChronoUnit.MINUTES);
        return dataAccessClient.getLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, inactiveSince);
    }

    private List<ObjectNode> getUpdatedPlan(Triplet<ReallocatedPlan, List<LineItem>, List<PbsHost>> triplet,
                                            PlanRequest planRequest) {
        ReallocatedPlan reallocatedPlan = triplet.getValue0();
        Map<String, Double> reallocationMap = getReallocationMap(reallocatedPlan);
        List<ObjectNode> updatedLineItems = new ArrayList<>();
        int activeHosts = triplet.getValue2().size();

        if (CollectionUtils.isEmpty(reallocationMap)) {
            metrics.incCounter(metricName("line-items-served"), updatedLineItems.size());
            metrics.incCounter(metricName("requests-served"));
            return updatedLineItems;
        }

        for (LineItem li : triplet.getValue1()) {
            ObjectNode liNode = li.getLineItemJson();
            JsonUtil.setValue(liNode, Constants.FIELD_EXT_LINE_ITEM_ID, li.getLineItemId());
            JsonUtil.setValue(liNode, Constants.FIELD_LINE_ITEM_ID, li.getBidderCode() + "-" + li.getLineItemId());
            JsonUtil.setValue(liNode, Constants.FIELD_SOURCE, li.getBidderCode());

            checkAndUpdateDeliverySchedule(liNode, reallocationMap, li.getUniqueLineItemId(), activeHosts);
            updatedLineItems.add(liNode);

            String accountId = JsonUtil.optString(liNode, Constants.FIELD_ACCOUNT_ID);
            if (tracer.checkActive()
                    && tracer.matchLineItemId(li.getLineItemId())
                    && tracer.matchBidderCode(li.getBidderCode())
                    && tracer.matchAccount(accountId)) {
                logger.info("{0}::{1}::{2}", GPConstants.TRACER, planRequest, liNode.toString());
            }
        }

        metrics.incCounter(metricName("requests-served"));
        metrics.incCounter(metricName("line-items-served"), updatedLineItems.size());
        return updatedLineItems;
    }

    private void checkAndUpdateDeliverySchedule(
            ObjectNode liNode, Map<String, Double> reallocationMap, String uniqueLineItemId, int activeHosts) {

        JsonNode schedulesJson = liNode.get(Constants.FIELD_DELIVERY_SCHEDULES);
        if (!(schedulesJson instanceof ArrayNode)) {
            return;
        }

        ArrayNode scheduleArray = (ArrayNode) schedulesJson;
        if (scheduleArray.size() == 0) {
            return;
        }

        Iterator<JsonNode> iterator = scheduleArray.elements();
        while (iterator.hasNext()) {
            JsonNode schedule = iterator.next();
            if (schedule instanceof ObjectNode) {
                ObjectNode scheduleObj = (ObjectNode) schedule;
                JsonNode tokensJson = scheduleObj.get(Constants.FIELD_TOKENS);
                if (tokensJson instanceof ArrayNode) {
                    ArrayNode tokensArray = (ArrayNode) tokensJson;
                    for (JsonNode token : tokensArray) {
                        ObjectNode tokenNode = (ObjectNode) token;
                        Integer clazz = JsonUtil.optInt(tokenNode, Constants.FIELD_CLASS);

                        if (clazz == 1) {
                            Integer oldTokens = JsonUtil.optInt(tokenNode, Constants.FIELD_TOTAL);
                            if (oldTokens != null) {
                                int reallocatedTokens = updatePlanTokens(oldTokens, reallocationMap,
                                        uniqueLineItemId, activeHosts);
                                JsonUtil.setValue(tokenNode, Constants.FIELD_TOTAL, reallocatedTokens);
                            }
                        }
                    }
                }
            }
        }
    }

    int updatePlanTokens(int planTokens, Map<String, Double> reallocationMap, String uniqueLineItemId,
                         int activeHosts) {
        if (planTokens == 0) {
            return 0;
        }
        int intTokens = planTokens;
        double tokens = planTokens;
        if (reallocationMap.containsKey(uniqueLineItemId)) {
            // already reallocated
            tokens = (double) planTokens * reallocationMap.get(uniqueLineItemId) / 100;
            intTokens = (int) tokens;
        } else if (activeHosts > 0) {
            // new line item without reallocation
            tokens = (double) planTokens / activeHosts;
            intTokens = (int) tokens;
        }
        if (tokens - intTokens < 0.00001) {
            return intTokens;
        }
        return random.nextDouble() <= (tokens - intTokens) ? (intTokens + 1) : intTokens;
    }

    private Map<String, Double> getReallocationMap(ReallocatedPlan reallocatedPlan) {
        if (reallocatedPlan == null || reallocatedPlan.isEmpty()) {
            return new HashMap<>();
        }
        return reallocatedPlan.getReallocationWeights().getWeights().stream()
                .collect(Collectors.toMap(Weightage::getUniqueLineItemId, Weightage::getWeight));
    }

    private <T> void finalHandler(AsyncResult<T> asyncResult, RoutingContext routingContext, long startTime) {
        HttpServerResponse response = routingContext.response();
        int statusCode = HttpResponseStatus.OK.code();

        List<String> errorDetails = new ArrayList<>();

        if (asyncResult.succeeded()) {
            response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            if (asyncResult.result() != null) {
                String jsonResp = Json.encode(asyncResult.result());
                if (tracer.checkActiveAndRaw()) {
                    PlanRequest planRequest = routingContext.get(PLAN_REQUEST_KEY);
                    if (planRequest != null
                            && tracer.matchVendor(planRequest.getVendor())
                            && tracer.matchRegion(planRequest.getRegion())) {
                        logger.info("{0}::Response to plan request::{1}", GPConstants.TRACER, jsonResp);
                    }
                }
                logger.info("GetPlans response time::{0}ms", System.currentTimeMillis() - startTime);
                logger.debug("GetPlans response::{0}", jsonResp);
                response.setStatusCode(statusCode).end(jsonResp);
            } else {
                response.setStatusCode(HttpResponseStatus.NO_CONTENT.code()).end();
            }
        } else {
            logger.error("Exception in executing PlanRequestHandler::{0}", asyncResult.cause().getStackTrace());
            if (asyncResult.cause() instanceof InvalidRequestException) {
                InvalidRequestException appExc = (InvalidRequestException) asyncResult.cause();
                if (appExc.getCode().equalsIgnoreCase(GPConstants.BAD_FORMAT)) {
                    statusCode = HttpResponseStatus.BAD_REQUEST.code();
                } else {
                    statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
                }
                errorDetails.add(((InvalidRequestException) asyncResult.cause()).getException());
            } else {
                statusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
                errorDetails.add(maskedErrorMessage);
                alertHttpClient.raiseEvent(
                        Constants.GP_PLANNER_PLAN_REQUEST_HANDLER_ERROR,
                        AlertPriority.MEDIUM,
                        String.format("Exception in PlanRequestHandler::%s", asyncResult.cause().getMessage()));
            }

            response.setStatusCode(statusCode)
                    .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);

            if (!errorDetails.isEmpty()) {
                response.end(Json.encode(errorDetails));
                metrics.incCounter(metricName("exc"));
            } else {
                response.end();
            }
        }
        metrics.updateTimer(metricName("processing-time"), System.currentTimeMillis() - startTime);
    }

    private String metricName(String tag) {
        return String.format("pbs-plan-request.%s", tag);
    }

}
