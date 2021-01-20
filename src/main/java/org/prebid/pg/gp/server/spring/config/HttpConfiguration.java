package org.prebid.pg.gp.server.spring.config;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredDeliveryDataHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.http.DeliveryDataHttpClient;
import org.prebid.pg.gp.server.http.PlannerAdapterHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.spring.config.app.AlertProxyConfiguration;
import org.prebid.pg.gp.server.spring.config.app.DeliveryDataConfiguration;
import org.prebid.pg.gp.server.spring.config.app.DeploymentConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration for http client related objects.
 */

@Configuration
class HttpConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HttpConfiguration.class);

    @Bean
    Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> circuitBreakerSecuredPlannerAdapterHttpClients(
            Vertx vertx,
            HttpClientConfiguration httpClientConfig,
            PlannerAdapterConfigurations paCfgs,
            Metrics metrics,
            AdminTracer adminTracer,
            AlertProxyHttpClient alertHttpClient
    ) {
        final Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> httpClientsMap = new HashMap<>();

        paCfgs.getPlanners()
                .stream()
                .forEach(paCfg -> {
                    PlannerCircuitBreaker breaker = new PlannerCircuitBreaker(
                            String.format("gp-%s-planner-adapter-cb", paCfg.getName()),
                            vertx,
                            httpClientConfig.getCircuitBreaker());
                    httpClientsMap.put(
                            paCfg.getName(),
                            new CircuitBreakerSecuredPlannerAdapterHttpClient(
                                new PlannerAdapterHttpClient(
                                    vertx, httpClientConfig, paCfg, metrics, adminTracer, alertHttpClient
                                ),
                                breaker
                            )
                    );
                });

        logger.info("CircuitBreakerSecuredPlannerAdapterHttpClients created for {0}", httpClientsMap.keySet());
        return httpClientsMap;
    }

    @Bean
    CircuitBreakerSecuredDeliveryDataHttpClient circuitBreakerSecuredDeliveryDataHttpClient(
            Vertx vertx,
            HttpClientConfiguration httpClientConfig,
            DeliveryDataConfiguration deliveryDataConfiguration,
            Metrics metrics,
            AdminTracer adminTracer,
            AlertProxyHttpClient alertHttpClient
    ) {
        final PlannerCircuitBreaker breaker = new PlannerCircuitBreaker(
                "gp-delivery-data-cb", vertx, httpClientConfig.getCircuitBreaker());
        return new CircuitBreakerSecuredDeliveryDataHttpClient(
                new DeliveryDataHttpClient(
                        vertx,
                        httpClientConfig,
                        deliveryDataConfiguration,
                        metrics,
                        adminTracer,
                        alertHttpClient),
                breaker);
    }

    @Bean
    AlertProxyHttpClient alertProxyHttpClient(
            Vertx vertx,
            HttpClientConfiguration httpClientConfig,
            DeploymentConfiguration deploymentConfiguration,
            AlertProxyConfiguration alertProxyConfiguration
    ) {
        return new AlertProxyHttpClient(vertx,
                httpClientConfig,
                deploymentConfiguration,
                alertProxyConfiguration,
                new ConcurrentHashMap<>());
    }

}

