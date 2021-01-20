package org.prebid.pg.gp.server.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.prebid.pg.gp.server.exception.GeneralPlannerException;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredDeliveryDataHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendStats;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.app.DeliveryDataConfiguration;
import org.prebid.pg.gp.server.util.Constants;
import org.prebid.pg.gp.server.util.Validators;
import org.springframework.util.StringUtils;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A service responsible for retrieval of line item delivery statistics data from stats servers.
 */
public class DeliveryDataService {

    private static final Logger logger = LoggerFactory.getLogger(DeliveryDataService.class);

    private static final String DATA_COUNT_METRIC = metricName("data-count");

    private static final String PROCESSING_TIME_METRIC = metricName("processing-time");

    private final Vertx vertx;

    private final Metrics metrics;

    private final AdminTracer tracer;

    private final Shutdown shutdown;

    private final DeliveryDataConfiguration deliveryDataConfig;

    private final CircuitBreakerSecuredDeliveryDataHttpClient deliveryDataHttpClient;

    private final CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient;

    private final AlertProxyHttpClient alertHttpClient;

    private final StatsCache statsCache;

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    private final int pbsMaxIdlePeriodInSeconds;

    private int startTimeInPastSec;

    public DeliveryDataService(
            Vertx vertx,
            DeliveryDataConfiguration deliveryDataConfig,
            int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            CircuitBreakerSecuredDeliveryDataHttpClient deliveryDataHttpClient,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertHttpClient,
            StatsCache statsCache
    ) {
        this.vertx = Objects.requireNonNull(vertx);
        this.tracer = Objects.requireNonNull(adminTracer);
        this.plannerDataAccessClient = Objects.requireNonNull(plannerDataAccessClient);
        this.deliveryDataConfig = Objects.requireNonNull(deliveryDataConfig);
        this.pbsMaxIdlePeriodInSeconds = pbsMaxIdlePeriodInSeconds;
        this.deliveryDataHttpClient = Objects.requireNonNull(deliveryDataHttpClient);
        this.metrics = Objects.requireNonNull(metrics);
        this.alertHttpClient = alertHttpClient;
        this.shutdown = shutdown;
        this.statsCache = Objects.requireNonNull(statsCache);

        logger.info(deliveryDataConfig.toString());
        logger.info(tracer.toString());
    }

    public void initialize() {
        if (!deliveryDataConfig.getEnabled()) {
            logger.info("algorithm_test, disabling periodic delivery data retrieval service");
            return;
        }
        vertx.setTimer(deliveryDataConfig.getInitialDelaySec() * 1000L, id -> {
            refreshDeliveryData("");
            vertx.setPeriodic(deliveryDataConfig.getRefreshPeriodSec() * 1000L,
                    timerId -> refreshDeliveryData(""));
        });
    }

    protected Future<Void> refreshDeliveryData(String simTime) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            logger.info("refreshDeliveryData::Server shutdown has been initiated");
            return Future.succeededFuture();
        }

        logger.info(tracer.toString());

        if (tracer.checkActive()) {
            logger.info("{0}::Refreshing delivery stats::{1} ... ",
                    GPConstants.TRACER, deliveryDataConfig.toString());
        } else {
            logger.info("Refreshing delivery stats::{0} ... ", deliveryDataConfig.toString());
        }

        final long start = System.currentTimeMillis();
        boolean isSim = !StringUtils.isEmpty(simTime);
        Instant now = isSim ? Instant.parse(simTime) : Instant.now();
        String updateSince = isSim ? now.minusSeconds(startTimeInPastSec).toString() : "";
        Instant activeSince = now.minusSeconds(pbsMaxIdlePeriodInSeconds);
        return plannerDataAccessClient.findActiveHosts(activeSince)
                .map(this::toVendorRegions)
                .compose(vendorRegions -> refresh(vendorRegions, start, updateSince, simTime));
    }

    private Future<Void> refresh(List<VendorRegion> vendorRegions, long start, String updateSince, String simTime) {
        if (vendorRegions.isEmpty()) {
            return Future.failedFuture("There are no active host regions available now.");
        }
        Future<Void> future = Future.future();

        final List<DeliveryTokenSpendSummary> deliveryStats = new ArrayList<>();
        Future<List<DeliveryTokenSpendSummary>> statsFuture =
                refreshForVendorRegion(vendorRegions.get(0), updateSince, simTime);
        for (int i = 1; i < vendorRegions.size(); i++) {
            final int index = i;
            statsFuture = statsFuture.compose(stats -> {
                deliveryStats.addAll(stats);
                return refreshForVendorRegion(vendorRegions.get(index), updateSince, simTime);
            });
        }
        statsFuture.setHandler(asyncStats -> {
            metrics.updateTimer(PROCESSING_TIME_METRIC, System.currentTimeMillis() - start);
            if (asyncStats.succeeded()) {
                deliveryStats.addAll(asyncStats.result());
                statsCache.set(deliveryStats);
                future.complete();
            } else {
                logger.error("Error while retrieving delivery stats", asyncStats.cause());
                future.fail(asyncStats.cause());
            }
        });
        return future;
    }

    private Future<List<DeliveryTokenSpendSummary>> refreshForVendorRegion(
            VendorRegion vendorRegion, String updateSince, String simTime) {
        String url = new StringBuilder(deliveryDataConfig.getUrl())
                .append("?vendor=").append(vendorRegion.vendor)
                .append("&region=").append(vendorRegion.region)
                .toString();
        return deliveryDataHttpClient
                .request(HttpMethod.GET, url, deliveryDataConfig.getUsername(),
                        deliveryDataConfig.getPassword(), updateSince)
                .compose(httpResponseContainer -> processHttpResponse(httpResponseContainer, simTime));
    }

    private Future<List<DeliveryTokenSpendSummary>> processHttpResponse(
            HttpResponseContainer httpResponseContainer, String simTime) {
        final int statusCode = httpResponseContainer.getStatusCode();

        if (statusCode < 200 || statusCode >= 300) {
            logger.error("{0}::{1}", statusCode, deliveryDataConfig.getUrl());
            metrics.incCounter("http.exc");
            metrics.incCounter(DATA_COUNT_METRIC, 0);

            String msg = String.format(
                    "Non-200 HTTP status from delivery stats service at %s::%s",
                    deliveryDataConfig.getUrl(), statusCode);
            alertHttpClient.raiseEvent(Constants.GP_PLANNER_DEL_STATS_CLIENT_ERROR, AlertPriority.HIGH, msg);
            return Future.failedFuture(new Exception(msg));
        }

        if (StringUtils.isEmpty(httpResponseContainer.getBody())) {
            logger.error("Empty stats from delivery stats service");
            if (statusCode != 204) {
                logger.error(
                        "Empty stats from delivery stats service::{0}::{1}",
                        statusCode, deliveryDataConfig.getUrl()
                );
                metrics.incCounter("http.exc");
                metrics.incCounter(DATA_COUNT_METRIC, 0);
                return Future.failedFuture(
                        new Exception(String.format("Empty stats from %s", deliveryDataConfig.getUrl())));
            } else {
                metrics.incCounter(DATA_COUNT_METRIC, 0);
                return Future.succeededFuture();
            }
        }

        DeliveryTokenSpendStats deliveryStats;
        try {
            deliveryStats = Json.mapper.readValue(httpResponseContainer.getBody(), DeliveryTokenSpendStats.class);
        } catch (Exception ex) {
            metrics.incCounter("json.exc");
            String msg = String.format(
                    "Exception while mapping DeliveryStats from JSON::%s, %s",
                    httpResponseContainer.getBody(), ex.getMessage()
            );
            logger.error(msg);
            metrics.incCounter(DATA_COUNT_METRIC, 0);
            return Future.failedFuture(new GeneralPlannerException(msg, ex));
        }

        logger.info(
                "Received {0} token spend summaries from {1}",
                deliveryStats.getTokenSpendSummaryLines().size(), deliveryDataConfig.getUrl()
        );

        metrics.incCounter(DATA_COUNT_METRIC, deliveryStats.getTokenSpendSummaryLines().size());

        if (tracer.checkActive()) {
            for (DeliveryTokenSpendSummary deliveryTokenSpendSummary : deliveryStats.getTokenSpendSummaryLines()) {
                if (tracer.matchRegion(deliveryTokenSpendSummary.getRegion())
                        && tracer.matchBidderCode(deliveryTokenSpendSummary.getBidderCode())
                        && tracer.matchVendor(deliveryTokenSpendSummary.getVendor())) {
                    logger.info("{0}::{1}", GPConstants.TRACER, deliveryTokenSpendSummary.toString());
                }
            }
        }

        Instant updatedAt = StringUtils.isEmpty(simTime) ? Instant.now() : Instant.parse(simTime);
        List<DeliveryTokenSpendSummary> validStats = new ArrayList<>();
        Map<DeliveryTokenSpendSummary, DeliveryTokenSpendSummary> statsMap = new HashMap<>();
        for (DeliveryTokenSpendSummary stats : deliveryStats.getTokenSpendSummaryLines()) {
            stats.setUpdatedAt(updatedAt);
            if (statsMap.containsKey(stats)) {
                logger.warn("DeliveryDataService::duplicated::{0}\n{1}", stats, statsMap.get(stats));
            } else {
                statsMap.put(stats, stats);
            }
            Set<ConstraintViolation<DeliveryTokenSpendSummary>> violations = validator.validate(stats);
            if (violations.isEmpty()) {
                validStats.add(stats);
            } else {
                logger.warn("Drop invalid stats data::{0}. Error::{1}",
                        stats, Validators.extractErrorMessages(violations));
            }
        }
        return Future.succeededFuture(validStats);
    }

    public void setStartTimeInPastSec(int startTimeInPastSec) {
        this.startTimeInPastSec = startTimeInPastSec;
    }

    public DeliveryDataConfiguration getDeliveryDataConfiguration() {
        return this.deliveryDataConfig;
    }

    public CircuitBreakerSecuredDeliveryDataHttpClient getDeliveryDataHttpClient() {
        return this.deliveryDataHttpClient;
    }

    private static String metricName(String tag) {
        return String.format("delivery-data-adapter.data-request.%s", tag);
    }

    private List<VendorRegion> toVendorRegions(List<PbsHost> hosts) {
        Set<VendorRegion> vendorRegions = hosts.stream()
                .map(host -> VendorRegion.of(host.getVendor(), host.getRegion()))
                .collect(Collectors.toSet());
        logger.info("VendorRegions::{0}", vendorRegions);
        return new ArrayList<>(vendorRegions);
    }

    @AllArgsConstructor(staticName = "of")
    @EqualsAndHashCode
    @ToString
    static class VendorRegion {

        private final String vendor;

        private final String region;

    }
}

