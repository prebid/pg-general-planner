package org.prebid.pg.gp.server.spring.config;

import com.mysql.cj.core.util.StringUtils;
import io.vertx.core.Vertx;
import org.prebid.pg.gp.server.auth.BasicAuthProvider;
import org.prebid.pg.gp.server.exception.GeneralPlannerException;
import org.prebid.pg.gp.server.handler.AppHealthCheckHandler;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredDeliveryDataHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.jdbc.LineItemsHistoryClient;
import org.prebid.pg.gp.server.jdbc.LineItemsTokensSummaryClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.services.DeliveryDataService;
import org.prebid.pg.gp.server.services.HostBasedTokenReallocation;
import org.prebid.pg.gp.server.services.HostReallocationService;
import org.prebid.pg.gp.server.services.LineItemsTokensSummaryService;
import org.prebid.pg.gp.server.services.PlannerAdapterServices;
import org.prebid.pg.gp.server.services.StatsCache;
import org.prebid.pg.gp.server.services.algotest.DeliveryDataServiceAlgoTest;
import org.prebid.pg.gp.server.services.algotest.HostAllocationServiceAlgoTest;
import org.prebid.pg.gp.server.services.algotest.PlannerAdapterServicesAlgoTest;
import org.prebid.pg.gp.server.spring.config.app.AlgorithmConfiguration;
import org.prebid.pg.gp.server.spring.config.app.DeliveryDataConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration;
import org.prebid.pg.gp.server.spring.config.app.ServerAuthDataConfiguration.Principal;
import org.prebid.pg.gp.server.spring.config.app.TokensSummaryConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Configuration for service layer related objects.
 */

@Configuration
class ServiceConfiguration {

    @Bean
    StatsCache statsCache() {
        return new StatsCache();
    }

    @SuppressWarnings({"squid:S00112"})
    @Bean
    HostBasedTokenReallocation hostBasedAllocation(HostReallocationConfiguration hostReallocationConfig)
            throws Exception {
        return (HostBasedTokenReallocation) Class.forName(hostReallocationConfig.getAlgorithm())
                .getConstructor(AlgorithmConfiguration.class)
                .newInstance(hostReallocationConfig.getAlgorithmSpec());
    }

    @Bean
    HostReallocationService hostReallocationService(
            Vertx vertx,
            HostReallocationConfiguration hostReallocationConfiguration,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            HostBasedTokenReallocation hostBasedAllocation,
            Metrics metrics,
            Shutdown shutdown,
            StatsCache statsCache) {
        return new HostReallocationService(
                vertx,
                hostReallocationConfiguration,
                pbsMaxIdlePeriodInSeconds,
                plannerDataAccessClient,
                hostBasedAllocation,
                metrics,
                shutdown,
                statsCache);
    }

    @Bean
    PlannerAdapterServices plannerAdapterServices(
            Vertx vertx,
            @Qualifier("serviceInstanceId") String serviceInstanceId,
            PlannerAdapterConfigurations plannerAdapterConfigurations,
            Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> plannerAdapterHttpClients,
            CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredJdbcClient,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient
    ) {
        PlannerAdapterServices plannerAdapterServices = new PlannerAdapterServices(vertx);
        plannerAdapterServices.initialize(
                serviceInstanceId,
                plannerAdapterConfigurations,
                circuitBreakerSecuredJdbcClient,
                plannerAdapterHttpClients,
                metrics,
                adminTracer,
                shutdown,
                alertProxyHttpClient
        );
        return plannerAdapterServices;
    }

    @Bean
    DeliveryDataService deliveryDataService(
            Vertx vertx,
            DeliveryDataConfiguration deliveryDataConfiguration,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            CircuitBreakerSecuredDeliveryDataHttpClient deliveryDataHttpClient,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient,
            StatsCache statsCache
    ) {

        DeliveryDataService deliveryDataService = new DeliveryDataService(
                vertx,
                deliveryDataConfiguration,
                pbsMaxIdlePeriodInSeconds,
                plannerDataAccessClient,
                deliveryDataHttpClient,
                metrics,
                adminTracer,
                shutdown,
                alertProxyHttpClient,
                statsCache);

        deliveryDataService.initialize();
        return deliveryDataService;
    }

    @Bean
    BasicAuthProvider basicHttpAuthProvider(ServerAuthDataConfiguration serverAuthDataConfig) {
        if (serverAuthDataConfig == null || serverAuthDataConfig.getPrincipals() == null) {
            throw new GeneralPlannerException("Failure in reading server-auth.principals");
        }
        for (Principal principal : serverAuthDataConfig.getPrincipals()) {
            if (StringUtils.isNullOrEmpty(principal.getUsername())
                    || StringUtils.isNullOrEmpty(principal.getPassword())
                    || StringUtils.isNullOrEmpty(principal.getRoles())) {
                throw new GeneralPlannerException("Failure in reading server-auth.principals entries");
            }
        }
        return new BasicAuthProvider(serverAuthDataConfig);
    }

    @Bean
    LineItemsTokensSummaryService lineItemHistorySummaryService(
            Vertx vertx,
            LineItemsHistoryClient lineItemsHistoryClient,
            LineItemsTokensSummaryClient tokensSummaryClient,
            AlertProxyHttpClient alertHttpClient,
            TokensSummaryConfiguration tokensSummaryConfiguration,
            Shutdown shutdown
    ) {
        LineItemsTokensSummaryService lineItemHistorySummaryService =
                new LineItemsTokensSummaryService(
                        vertx,
                        lineItemsHistoryClient,
                        tokensSummaryClient,
                        alertHttpClient,
                        tokensSummaryConfiguration,
                        shutdown);
        lineItemHistorySummaryService.initialize();
        return lineItemHistorySummaryService;
    }

    @Bean
    HostAllocationServiceAlgoTest hostAllocationServiceAlgoTest(
            Vertx vertx,
            HostReallocationConfiguration hostReallocationConfiguration,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            HostBasedTokenReallocation hostBasedAllocation,
            Shutdown shutdown,
            StatsCache statsCache) {
        return new HostAllocationServiceAlgoTest(
                vertx,
                hostReallocationConfiguration,
                pbsMaxIdlePeriodInSeconds,
                plannerDataAccessClient,
                hostBasedAllocation,
                shutdown,
                statsCache);
    }

    @Bean
    DeliveryDataServiceAlgoTest deliveryDataServiceAlgoTest(
            Vertx vertx,
            DeliveryDataConfiguration deliveryDataConfiguration,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            CircuitBreakerSecuredDeliveryDataHttpClient deliveryDataHttpClient,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient,
            StatsCache statsCache,
            @Value("${services.algotest.delivery-data.start-time-in-past-sec}") int startTimeInPastSec) {
        return new DeliveryDataServiceAlgoTest(
                vertx,
                deliveryDataConfiguration,
                pbsMaxIdlePeriodInSeconds,
                plannerDataAccessClient,
                deliveryDataHttpClient,
                metrics,
                adminTracer,
                shutdown,
                alertProxyHttpClient,
                statsCache,
                startTimeInPastSec);
    }

    @Bean
    PlannerAdapterServicesAlgoTest plannerAdapterServicesAlgoTest(
            Vertx vertx,
            @Qualifier("serviceInstanceId") String serviceInstanceId,
            PlannerAdapterConfigurations plannerAdapterConfigurations,
            Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> plannerAdapterHttpClients,
            CircuitBreakerSecuredPlannerDataAccessClient circuitBreakerSecuredJdbcClient,
            AppHealthCheckHandler healthCheckHandler,
            Metrics metrics,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient,
            @Value("${services.algotest.planner-adapters.future-plan-hours}") int simFuturePlanHours
    ) {
        PlannerAdapterServicesAlgoTest plannerAdapterServices = new PlannerAdapterServicesAlgoTest(vertx);
        plannerAdapterServices.initialize(
                serviceInstanceId,
                plannerAdapterConfigurations,
                circuitBreakerSecuredJdbcClient,
                plannerAdapterHttpClients,
                metrics,
                adminTracer,
                shutdown,
                alertProxyHttpClient,
                simFuturePlanHours
        );
        return plannerAdapterServices;
    }

}

