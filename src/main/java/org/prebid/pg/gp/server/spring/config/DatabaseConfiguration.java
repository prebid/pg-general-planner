package org.prebid.pg.gp.server.spring.config;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import org.prebid.pg.gp.server.exception.GeneralPlannerException;
import org.prebid.pg.gp.server.handler.AppHealthCheckHandler;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.AdminEventClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.jdbc.LineItemsClient;
import org.prebid.pg.gp.server.jdbc.LineItemsHistoryClient;
import org.prebid.pg.gp.server.jdbc.LineItemsTokensSummaryClient;
import org.prebid.pg.gp.server.jdbc.PlannerDataAccessClient;
import org.prebid.pg.gp.server.jdbc.ReallocatedPlansClient;
import org.prebid.pg.gp.server.jdbc.RegistrationClient;
import org.prebid.pg.gp.server.jdbc.SystemStateClient;
import org.prebid.pg.gp.server.jdbc.TokenSpendClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.spring.config.app.LineItemsTokensSummaryConfiguration;
import org.prebid.pg.gp.server.spring.config.app.DeploymentConfiguration;
import org.prebid.pg.gp.server.spring.config.app.PlannerDatabaseProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.Inet4Address;

/**
 * Configuration for database access, data persistence and retrieval related objects.
 */

@Configuration
class DatabaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfiguration.class);

    private static final String INFRA_ECS = "ecs";

    private static final String NA_SERVICE_INSTANCE_ID = "NA";

    @Bean(name = "serviceInstanceId")
    String serviceInstanceId(DeploymentConfiguration deployment) {
        if (INFRA_ECS.equals(deployment.getInfra())) {
            return NA_SERVICE_INSTANCE_ID;
        }
        try {
            return Inet4Address.getLocalHost().getHostName();
        } catch (Exception e) {
            throw new GeneralPlannerException("Exception in reading hostname", e);
        }
    }

    @Bean
    CircuitBreakerSecuredPlannerDataAccessClient plannerCircuitBreakerSecuredJdbcClient(
            Vertx vertx, PlannerDataAccessClient plannerDataAccessClient,
            PlannerDatabaseProperties plannerDatabaseProperties,
            AppHealthCheckHandler healthCheckHandler) {
        final PlannerCircuitBreaker breaker = new PlannerCircuitBreaker(
                "gp-jdbc-client-circuit-breaker", vertx, plannerDatabaseProperties.getCircuitBreaker());
        final CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient =
                new CircuitBreakerSecuredPlannerDataAccessClient(plannerDataAccessClient, breaker);
        healthCheckHandler.register(dataAccessClient);
        return dataAccessClient;
    }

    @Bean
    JDBCClient jdbcClient(Vertx vertx, PlannerDatabaseProperties plannerDatabaseProperties) {
        final String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        final String jdbcUrlPrefix = "jdbc:mysql:";
        final String jdbcUrlSuffix = "useSSL=false&tcpKeepAlive=true";

        final String jdbcUrl = String.format("%s//%s:%d/%s?%s",
                jdbcUrlPrefix,
                plannerDatabaseProperties.getHost(),
                plannerDatabaseProperties.getPort(),
                plannerDatabaseProperties.getDbname(),
                jdbcUrlSuffix);

        logger.info("plannerDataAccessClient::jdbcUrl={0}", jdbcUrl);
        logger.info("plannerDataAccessClient::user={0}", plannerDatabaseProperties.getUser());
        logger.info("plannerDataAccessClient::initial-pool-size={0}", plannerDatabaseProperties.getInitialPoolSize());
        logger.info("plannerDataAccessClient::min-pool-size={0}", plannerDatabaseProperties.getMinPoolSize());
        logger.info("plannerDataAccessClient::max-pool-size={0}", plannerDatabaseProperties.getMaxPoolSize());
        logger.info("plannerDataAccessClient::max-idle-time-secs={0}", plannerDatabaseProperties.getMaxIdleTimeSec());

        return JDBCClient.createShared(vertx, new JsonObject()
                .put("url", jdbcUrl)
                .put("user", plannerDatabaseProperties.getUser())
                .put("password", plannerDatabaseProperties.getPassword())
                .put("driver_class", jdbcDriver)
                .put("initial_pool_size", plannerDatabaseProperties.getInitialPoolSize())
                .put("min_pool_size", plannerDatabaseProperties.getMinPoolSize())
                .put("max_pool_size", plannerDatabaseProperties.getMaxPoolSize())
                .put("max_idle_time", plannerDatabaseProperties.getMaxIdleTimeSec() * 1000));
    }

    @Bean
    PlannerDataAccessClient plannerDataAccessClient(
            Vertx vertx,
            JDBCClient jdbcClient,
            LineItemsClient lineItemsClient,
            TokenSpendClient tokenSpendClient,
            SystemStateClient systemStateClient,
            RegistrationClient registrationClient,
            ReallocatedPlansClient reallocatedPlansClient,
            LineItemsTokensSummaryClient lineItemsTokensSummaryClient,
            LineItemsHistoryClient lineItemsHistoryClient,
            DeploymentConfiguration deployment,
            AdminEventClient adminCommandClient,
            Metrics metrics,
            AlertProxyHttpClient alertHttpClient,
            LineItemsTokensSummaryConfiguration lineItemsTokensSummaryConfiguration
    ) {
        return new PlannerDataAccessClient(
                jdbcClient,
                lineItemsClient,
                tokenSpendClient,
                systemStateClient,
                registrationClient,
                reallocatedPlansClient,
                lineItemsTokensSummaryClient,
                lineItemsHistoryClient,
                adminCommandClient,
                metrics,
                alertHttpClient,
                lineItemsTokensSummaryConfiguration
        );
    }

    @Bean
    LineItemsHistoryClient lineItemsHistoryClient(
            JDBCClient jdbcClient,
            Metrics metrics,
            SystemStateClient systemStateClient,
            AlertProxyHttpClient alertHttpClient
    ) {
        return new LineItemsHistoryClient(jdbcClient, metrics, systemStateClient, alertHttpClient);
    }

    @Bean
    LineItemsTokensSummaryClient tokensSummaryClient(
            JDBCClient jdbcClient,
            Metrics metrics,
            AlertProxyHttpClient alertHttpClient) {
        return new LineItemsTokensSummaryClient(jdbcClient, metrics, alertHttpClient);
    }

    @Bean
    LineItemsClient lineItemsClient(
            @Qualifier("serviceInstanceId") String serviceInstanceId, Metrics metrics, AdminTracer adminTracer
    ) {
        logger.info("serviceInstanceId::{0}", serviceInstanceId);
        return new LineItemsClient(serviceInstanceId, metrics, adminTracer);
    }

    @Bean
    TokenSpendClient tokenSpendClient(
            @Qualifier("serviceInstanceId") String serviceInstanceId, Metrics metrics
    ) {
        return new TokenSpendClient(serviceInstanceId, metrics);
    }

    @Bean
    SystemStateClient systemStateClient(Metrics metrics) {
        return new SystemStateClient(metrics);
    }

    @Bean
    RegistrationClient registrationClient(Metrics metrics, AdminTracer adminTracer) {
        return new RegistrationClient(metrics, adminTracer);
    }

    @Bean
    ReallocatedPlansClient reallocatedPlansClient(
            @Qualifier("serviceInstanceId") String serviceInstanceId, Metrics metrics, AdminTracer adminTracer
    ) {
        return new ReallocatedPlansClient(serviceInstanceId, metrics, adminTracer);
    }

    @Bean
    AdminEventClient adminEventClient(Metrics metrics) {
        return new AdminEventClient(metrics);
    }

}
