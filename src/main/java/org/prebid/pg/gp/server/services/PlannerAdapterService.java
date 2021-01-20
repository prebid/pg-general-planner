package org.prebid.pg.gp.server.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.model.SystemState;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations.PlannerAdapterConfiguration;
import org.prebid.pg.gp.server.util.Constants;
import org.prebid.pg.gp.server.util.JsonUtil;
import org.prebid.pg.gp.server.util.Validators;
import org.springframework.util.StringUtils;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A service to retieve line items information from Planner Adapter periodically.
 */
public class PlannerAdapterService {

    private static final Logger logger = LoggerFactory.getLogger(PlannerAdapterService.class);

    private static final String PROCESSING_TIME = "processing-time";

    private final Vertx vertx;

    private final Metrics metrics;

    private final AdminTracer tracer;

    private final Shutdown shutdown;

    private final Integer batchSize;

    private final String plannerAdapterRefreshTsTag;

    private final PlannerAdapterConfiguration plannerAdapterConfig;

    private final CircuitBreakerSecuredPlannerAdapterHttpClient plannerAdapterHttpClient;

    private final CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient;

    private final AlertProxyHttpClient alertProxyHttpClient;

    private int futurePlanHours;

    private Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    public PlannerAdapterService(
            Vertx vertx,
            String hostName,
            PlannerAdapterConfiguration plannerAdapterConfig,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            CircuitBreakerSecuredPlannerAdapterHttpClient plannerAdapterHttpClient,
            Integer batchSize,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient
    ) {
        this.vertx = Objects.requireNonNull(vertx);
        this.plannerAdapterConfig = Objects.requireNonNull(plannerAdapterConfig);
        this.plannerAdapterRefreshTsTag = String.format("plans_refresh_ts_%s_%s",
                plannerAdapterConfig.getName(), hostName);
        this.plannerDataAccessClient = Objects.requireNonNull(plannerDataAccessClient);
        this.plannerAdapterHttpClient = Objects.requireNonNull(plannerAdapterHttpClient);
        this.batchSize = Validators.checkArgument(batchSize, batchSize >= 1, "batchSize should larger than 0");
        this.tracer = Objects.requireNonNull(adminTracer);
        this.metrics = Objects.requireNonNull(metrics);
        this.alertProxyHttpClient = Objects.requireNonNull(alertProxyHttpClient);
        this.shutdown = Objects.requireNonNull(shutdown);

        logger.info(plannerAdapterConfig.toString());
    }

    /**
     * Set up timer to retrieve line items information from Planner Adapter periodically.
     */
    public void initialize() {
        if (!plannerAdapterConfig.getEnabled()) {
            logger.info("algorithm_test, disabling periodic plan retrieval service");
            return;
        }
        vertx.setTimer(plannerAdapterConfig.getInitialDelaySec() * 1000L, id -> {
            refreshPlans();
            vertx.setPeriodic(plannerAdapterConfig.getRefreshPeriodSec() * 1000L, timerId -> refreshPlans());
        });
    }

    public Future<UpdateResult> refreshPlans() {
        return refreshPlans(null);
    }

    public Future<UpdateResult> refreshPlans(String sinceTime) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            logger.info("PlannerAdapterService::Server shutdown has been initiated");
            return Future.succeededFuture();
        }

        logger.info(tracer);

        if (tracer.checkActive()) {
            logger.info("{0}::Refreshing plans::{1} ... ", GPConstants.TRACER, plannerAdapterConfig);
        } else {
            logger.info("Refreshing plans::{0} ... ", plannerAdapterConfig);
        }

        Instant now = Instant.now();
        SystemState systemState = SystemState.builder()
                .tag(plannerAdapterRefreshTsTag)
                .val(StringUtils.isEmpty(sinceTime) ? now.toString() : sinceTime)
                .build();
        Instant updatedAt = StringUtils.isEmpty(sinceTime) ? now : Instant.parse(sinceTime);
        return getPlans(sinceTime, updatedAt)
                .compose(v -> plannerDataAccessClient.updateSystemStateWithUTCTime(systemState));
    }

    private Future<Void> getPlans(String since, Instant updatedAt) {
        String url = plannerAdapterConfig.getUrl() + queryStrings(since);
        logger.info("URL=" + url);
        final long start = System.currentTimeMillis();
        return plannerAdapterHttpClient.request(
                HttpMethod.GET,
                url,
                plannerAdapterConfig.getUsername(),
                plannerAdapterConfig.getPassword(),
                since)
        .compose(ar -> {
            logger.info(
                    "HTTP {1} Planner Adaptor response time::{0}ms",
                    System.currentTimeMillis() - start, plannerAdapterConfig.getName()
            );
            return processHttpResponse(ar, start, updatedAt);
        });
    }

    protected String queryStrings(String since) {
        if (StringUtils.isEmpty(since)) {
            return "";
        }
        return String.format("?since=%s&hours=%s", since, futurePlanHours);
    }

    private Future<Void> processHttpResponse(HttpResponseContainer httpResponseContainer,
            long startTime, Instant now) {
        final int statusCode = httpResponseContainer.getStatusCode();

        if (!(statusCode >= 200 && statusCode <= 299)) {
            String msg = String.format(
                    "Non-200 HTTP status in request to planner adapter at %s hosted by %s::%s",
                    plannerAdapterConfig.getUrl(), plannerAdapterConfig.getName(), statusCode
            );
            logger.error(msg);
            alertProxyHttpClient.raiseEvent(Constants.GP_PLANNER_ADAPTER_CLIENT_ERROR, AlertPriority.HIGH, msg);
            return Future.failedFuture(msg);
        }

        if (httpResponseContainer.getBody() == null || httpResponseContainer.getBody().isEmpty()) {
            String msg = String.format("Empty plans received from %s hosted by %s",
                    plannerAdapterConfig.getName(), plannerAdapterConfig.getUrl());
            alertProxyHttpClient.raiseEvent(Constants.GP_PLANNER_ADAPTER_CLIENT_ERROR, AlertPriority.LOW, msg);

            if (statusCode != 204) {
                return Future.failedFuture(new Exception(String.format(msg)));
            } else {
                logger.warn("Empty plans::{0}::{1}", statusCode, plannerAdapterConfig.getUrl());
                metrics.updateTimer(metricName(PROCESSING_TIME), System.currentTimeMillis() - startTime);
                metrics.incCounter(metricName("line-item-count"), 0);
                return Future.succeededFuture();
            }
        }

        List<LineItem> lineItems = new ArrayList<>();
        try {
            List<ObjectNode> liNodes = Json.mapper.readValue(
                    httpResponseContainer.getBody(), new TypeReference<List<ObjectNode>>() { });

            for (ObjectNode node : liNodes) {
                String lineItemId = JsonUtil.optString(node, Constants.FIELD_LINE_ITEM_ID);
                LineItem lineItem = LineItem.from(node, plannerAdapterConfig.getName(),
                        plannerAdapterConfig.getBidderCodePrefix());
                lineItem.setUpdatedAt(now);
                // hard coded to active for all latest line items, real original status is in json object
                lineItem.setStatus(Constants.LINE_ITEM_ACTIVE_STATUS);
                Set<ConstraintViolation<LineItem>> violations = validator.validate(lineItem);
                if (violations.isEmpty()) {
                    lineItems.add(lineItem);
                } else {
                    logger.warn("Dropped invalid line item {0}::{1}::{2}",
                            lineItemId, Validators.extractErrorMessages(violations), lineItem
                    );
                }

                String accountId = JsonUtil.optString(node, Constants.FIELD_ACCOUNT_ID);
                if (tracer.checkActive()
                        && tracer.matchBidderCode(lineItem.getBidderCode())
                        && tracer.matchLineItemId(lineItem.getLineItemId())
                        && tracer.matchAccount(accountId)) {
                    logger.info("{0}::{1}", GPConstants.TRACER, lineItem.toString());
                }
            }

            int emptySchedules = liNodes.size() - lineItems.size();
            if (!tracer.checkActive()) {
                logger.info(
                        "Received {0} line items from {1}", liNodes.size(), plannerAdapterConfig.getName()
                );
                logger.info(
                        "Received {0} line items from {1} with current/future plans",
                        lineItems.size(), plannerAdapterConfig.getName()
                );
            } else {
                logger.info("Received {0} line items from {1}. {2} with empty schedules.",
                        liNodes.size(), plannerAdapterConfig.getName(), emptySchedules);
                logger.info(
                        "{0}::Received {1} line items from {2}. {3} with empty schedules.",
                        GPConstants.TRACER, lineItems.size(), plannerAdapterConfig.getName(), emptySchedules);
            }

            metrics.incCounter(metricName("line-item-count"), lineItems.size());
            metrics.incCounter(metricName("line-item-empty-schedule-count"), emptySchedules);

        } catch (Exception e) {
            String msg = String.format(
                    "Error parsing response from planner adapter at %s hosted by %s::%s",
                    plannerAdapterConfig.getName(), plannerAdapterConfig.getName(), e.getMessage()
            );
            logger.error("Error parsing json plans", e);
            alertProxyHttpClient
                    .raiseEvent(Constants.GP_PLANNER_ADAPTER_CLIENT_ERROR, AlertPriority.HIGH, msg)
                    .setHandler(response -> { });
            return Future.failedFuture("Error parsing json plans");
        }

        Future<Void> future = Future.future();
        if (!lineItems.isEmpty()) {
            plannerDataAccessClient.updateLineItems(lineItems, batchSize)
                    .setHandler(ar -> {
                        if (ar.succeeded()) {
                            logger.info("Saved {0} line items to DB.", lineItems.size());
                            metrics.updateTimer(metricName(PROCESSING_TIME), System.currentTimeMillis() - startTime);
                            future.complete();
                        } else {
                            future.fail("Received plans are all empty.");
                        }
                    });
        } else {
            metrics.updateTimer(metricName(PROCESSING_TIME), System.currentTimeMillis() - startTime);
            return Future.succeededFuture();
        }

        return future;
    }

    public void setFuturePlanHours(int futurePlanHours) {
        this.futurePlanHours = futurePlanHours;
    }

    public PlannerAdapterConfiguration getPlannerAdapterConfig() {
        return plannerAdapterConfig;
    }

    public CircuitBreakerSecuredPlannerAdapterHttpClient getPlannerAdapterHttpClient() {
        return plannerAdapterHttpClient;
    }

    private String metricName(String tag) {
        return String.format("planner-adapter.%s.line-item-request.%s", plannerAdapterConfig.getName(), tag);
    }
}

