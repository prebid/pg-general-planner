package org.prebid.pg.gp.server.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.javatuples.Pair;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;
import org.prebid.pg.gp.server.util.Constants;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A service to handle host-based token reallocation.
 */
public class HostReallocationService {

    private static final Logger logger = LoggerFactory.getLogger(HostReallocationService.class);

    private final Vertx vertx;

    private final Metrics metrics;

    private final Shutdown shutdown;

    private final HostReallocationConfiguration reallocationConfig;

    private final CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient;

    private final HostBasedTokenReallocation reallocator;

    private final StatsCache statsCache;

    private final int pbsMaxIdlePeriodInSeconds;

    public HostReallocationService(
            Vertx vertx,
            HostReallocationConfiguration reallocationConfig,
            int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            HostBasedTokenReallocation hostBasedReallocation,
            Metrics metrics,
            Shutdown shutdown,
            StatsCache statsCache
    ) {
        this.reallocationConfig = Objects.requireNonNull(reallocationConfig);
        this.vertx = Objects.requireNonNull(vertx);
        this.plannerDataAccessClient = Objects.requireNonNull(plannerDataAccessClient);
        this.pbsMaxIdlePeriodInSeconds = pbsMaxIdlePeriodInSeconds;
        this.reallocator = Objects.requireNonNull(hostBasedReallocation);
        this.shutdown = Objects.requireNonNull(shutdown);
        this.metrics = Objects.requireNonNull(metrics);
        this.statsCache = Objects.requireNonNull(statsCache);
        this.initialize();
    }

    /**
     * Set up timer to do host-based token reallocation periodically.
     */
    public void initialize() {
        logger.info(reallocationConfig.toString());

        if (!reallocationConfig.getEnabled()) {
            logger.info("algorithm_test, disabling host reallocation service");
            return;
        }
        vertx.setTimer(reallocationConfig.getInitialDelaySec() * 1000L, id -> {
            calculate(null);
            vertx.setPeriodic(reallocationConfig.getRefreshPeriodSec() * 1000L, timerId -> calculate(null));
        });
    }

    @SuppressWarnings({"squid:S1854", "squid:S1481"})
    protected Future<Void> calculate(Instant endTime) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            logger.info("HostReallocationService::Server shutdown has been initiated");
            return Future.succeededFuture();
        }

        logger.info("Start HostReallocationService calculation ...");
        Future<Void> future = Future.future();
        final long start = System.currentTimeMillis();

        Instant endTimestamp = endTime == null ? Instant.now() : endTime;
        plannerDataAccessClient.getCompactLineItemsByStatus(Constants.LINE_ITEM_ACTIVE_STATUS, endTimestamp)
                .compose(lineItems -> {
                    if (lineItems.isEmpty()) {
                        return Future.failedFuture("No active line items found.");
                    }
                    return plannerDataAccessClient
                            .findActiveHosts(hostsActiveSince())
                            .map(activeHosts -> new Pair<>(lineItems, activeHosts));
                })
                .compose(pair -> {
                    if (pair.getValue1().isEmpty()) {
                        return Future.failedFuture("No active hosts found.");
                    }
                    return reallocateShares(pair, endTimestamp);
                })
                .compose(reallocatedPlans -> {
                    logger.info("Starting update of reallocation plan tables");
                    return plannerDataAccessClient.updateReallocatedPlans(
                                reallocatedPlans, reallocationConfig.getDbStoreBatchSize());
                })
                .setHandler(
                        ar -> {
                            logger.info(
                                    "End HostReallocationService calculation::{0}ms",
                                    System.currentTimeMillis() - start
                            );
                            metrics.updateTimer(metricName("processing-time"), System.currentTimeMillis() - start);
                            if (ar.succeeded()) {
                                future.complete();
                            } else {
                                logger.info(ar.cause().getMessage());
                                future.fail(ar.cause());
                            }
                        });

        return future;
    }

    protected Instant hostsActiveSince() {
        return Instant.now().minusSeconds(pbsMaxIdlePeriodInSeconds);
    }

    private Future<List<ReallocatedPlan>> reallocateShares(
            Pair<List<LineItem>, List<PbsHost>> lineItemsActiveHostsPair, Instant now
    ) {
        final List<LineItem> activeLineItems = lineItemsActiveHostsPair.getValue0();
        final List<PbsHost> pbsHosts = lineItemsActiveHostsPair.getValue1();
        logger.info("Reallocating shares among {0} PBS hosts", pbsHosts.size());
        metrics.incCounter("counts.active-pbs-hosts", pbsHosts.size());

        Future<List<ReallocatedPlan>> future = Future.future();
        final List<DeliveryTokenSpendSummary> allStats = statsCache.get();

        Instant updatedSince = now.minus(reallocationConfig.getReallocationUpdatedSinceMin(), ChronoUnit.MINUTES);
        plannerDataAccessClient.getLatestReallocatedPlans(updatedSince)
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        List<ReallocatedPlan> prePlans = new ArrayList<>(ar.result());
                        if (prePlans.isEmpty() && !allStats.isEmpty()) {
                            // extreme case: has stats without previous plan, first allocate average shares, then
                            // migrate shares among slow and fast hosts based on stats, reduce algorithm complexity
                            prePlans = reallocator.calculate(new ArrayList<>(), prePlans, activeLineItems, pbsHosts);
                        }
                        List<ReallocatedPlan> reallocatedPlans =
                                reallocator.calculate(allStats, prePlans, activeLineItems, pbsHosts);
                        for (ReallocatedPlan plan : reallocatedPlans) {
                            plan.setUpdatedAt(now);
                        }
                        future.complete(reallocatedPlans);
                    } else {
                        future.fail("Error while retrieving relevant data.");
                    }
                });

        return future;
    }

    private String metricName(String tag) {
        return String.format("host-rellocation.%s", tag);
    }

}

