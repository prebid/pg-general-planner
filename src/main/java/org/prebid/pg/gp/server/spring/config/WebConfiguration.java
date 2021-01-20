package org.prebid.pg.gp.server.spring.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import org.prebid.pg.gp.server.auth.BasicAuthProvider;
import org.prebid.pg.gp.server.handler.AdminHandler;
import org.prebid.pg.gp.server.handler.AppHealthCheckHandler;
import org.prebid.pg.gp.server.handler.CeaseShutdownHandler;
import org.prebid.pg.gp.server.handler.GeneralHealthCheckHandler;
import org.prebid.pg.gp.server.handler.LineItemsTokensSummaryHandler;
import org.prebid.pg.gp.server.handler.PbsHealthHandler;
import org.prebid.pg.gp.server.handler.PbsRegistrationHandler;
import org.prebid.pg.gp.server.handler.PlanRequestHandler;
import org.prebid.pg.gp.server.handler.PrepShutdownHandler;
import org.prebid.pg.gp.server.handler.TrxIdHandler;
import org.prebid.pg.gp.server.handler.algotest.HostAllocationAlgoTestHandler;
import org.prebid.pg.gp.server.handler.algotest.PlannerAdaptersUpdateAlgoTestHandler;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.services.algotest.DeliveryDataServiceAlgoTest;
import org.prebid.pg.gp.server.services.algotest.HostAllocationServiceAlgoTest;
import org.prebid.pg.gp.server.services.algotest.PlannerAdapterServicesAlgoTest;
import org.prebid.pg.gp.server.spring.config.app.DeploymentConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HostReallocationConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import javax.annotation.PostConstruct;
import java.util.Random;

/**
 * Configuration for http request routing and handlers to serve http request.
 */

@Configuration
public class WebConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(WebConfiguration.class);

    @Autowired
    private Vertx vertx;

    @Autowired
    private Router router;

    @Value("${http.port}")
    private int httpPort;

    @Value("${http.idle-timeout-sec}")
    private int httpIdleTimeoutInSeconds;

    @PostConstruct
    public void startHttpServer() {
        final HttpServerOptions httpServerOptions = new HttpServerOptions()
                .setCompressionSupported(true)
                .setDecompressionSupported(true)
                .setIdleTimeout(httpIdleTimeoutInSeconds);

        vertx.createHttpServer(httpServerOptions)
                .requestHandler(router)
                .listen(httpPort);

        logger.info("Successfully started 1 instance of Http Server on port {0}", httpPort);
    }

    @Bean
    @Order()
    AppHealthCheckHandler appHealthCheckHandler(Vertx vertx) {
        return new AppHealthCheckHandler(vertx);
    }

    @Bean
    GeneralHealthCheckHandler generalHealthCheckHandler(Shutdown shutdown) {
        return new GeneralHealthCheckHandler(shutdown);
    }

    @Bean
    Router router(@Value("${http.base-url}") String baseURL,
                  @Value("${http.admin-base-url}") String baseAdminURL,
                  PbsRegistrationHandler pbsRegistrationHandler,
                  PlanRequestHandler planRequestHandler,
                  GeneralHealthCheckHandler generalHealthCheckHandler,
                  BasicAuthProvider basicAuthProvider,
                  HostAllocationAlgoTestHandler hostAllocationAlgoTestHandler,
                  LineItemsTokensSummaryHandler lineItemsTokensSummaryHandler,
                  DeploymentConfiguration deploymentConfiguration,
                  PlannerAdaptersUpdateAlgoTestHandler adaptersUpdateAlgoTestHandler,
                  AdminHandler adminHandler,
                  PbsHealthHandler pbsHealthHandler,
                  PrepShutdownHandler prepShutdownHandler,
                  CeaseShutdownHandler ceaseShutdownHandler
    ) {
        final Router appRouter = Router.router(vertx);
        final boolean isAlgoTestEnabled = "algotest".equals(deploymentConfiguration.getProfile());
        logger.info("Base URL::{0}", baseURL);

        appRouter.route().handler(CookieHandler.create());
        appRouter.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));
        appRouter.route().handler(BodyHandler.create());

        if (isAlgoTestEnabled) {
            logger.info("algotest enabled");
            appRouter.route(baseURL + "/e2eAdmin/hostAlloc").handler(hostAllocationAlgoTestHandler);
            appRouter.route(baseURL + "/e2eAdmin/updatePlan").handler(adaptersUpdateAlgoTestHandler);
        } else if (basicAuthProvider.getServerAuthDataConfiguration().isAuthenticationEnabled()) {
            AuthHandler basicAuthHandler = BasicAuthHandler.create(basicAuthProvider);
            appRouter.route(String.format("%s/register", baseURL)).handler(basicAuthHandler);
            appRouter.route(String.format("%s/plans", baseURL)).handler(basicAuthHandler);
            appRouter.post(String.format("%s/prep-for-shutdown", baseURL)).handler(basicAuthHandler);
            appRouter.post(String.format("%s/cease-shutdown", baseURL)).handler(basicAuthHandler);
            appRouter.post(String.format("%s/admin", baseURL)).handler(basicAuthHandler);
            appRouter.get(String.format("%s/pbs-health", baseURL)).handler(basicAuthHandler);
            appRouter.route(String.format("%s/line-items-tokens-summary", baseURL)).handler(basicAuthHandler);
        }

        TrxIdHandler trxIdHandler = new TrxIdHandler();
        appRouter.route(baseURL + "/*").handler(trxIdHandler);

        appRouter.get(String.format("%s/plans", baseURL)).handler(planRequestHandler);
        appRouter.post(String.format("%s/register", baseURL)).handler(pbsRegistrationHandler);
        appRouter.post(String.format("%s/prep-for-shutdown", baseURL)).handler(prepShutdownHandler);
        appRouter.post(String.format("%s/cease-shutdown", baseURL)).handler(ceaseShutdownHandler);
        appRouter.post(String.format("%s/admin", baseURL)).handler(adminHandler);
        appRouter.get(String.format("%s/pbs-health", baseURL)).handler(pbsHealthHandler);
        appRouter.get(String.format("%s/line-items-tokens-summary", baseURL)).handler(lineItemsTokensSummaryHandler);

        appRouter.get(String.format("%s/alive", baseURL)).handler(generalHealthCheckHandler);

        appRouter.getRoutes().stream().forEach(r -> logger.info("Router paths::" + r.getPath()));
        return appRouter;
    }

    @Bean
    AdminHandler adminHandler(
            @Value("${server-api-roles.tracer}") String resourceRole,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            AdminTracer adminTracer,
            @Value("${admin.tracer.max-duration-in-seconds:900}") int maxDurationInSeconds,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            @Value("${admin.apps}") String applicationListStr,
            @Value("${admin.db-store-batch-size}") int batchSize,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            Shutdown shutdown
    ) {
        return new AdminHandler(
                adminTracer,
                maxDurationInSeconds,
                pbsMaxIdlePeriodInSeconds,
                applicationListStr,
                batchSize,
                plannerDataAccessClient,
                resourceRole,
                securityEnabled,
                shutdown
        );
    }

    @Bean
    PlanRequestHandler planRequestHandler(
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            @Value("${error.message}") String maskedErrorMessage,
            @Value("${server-api-roles.plan-request}") String resourceRole,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            HostReallocationConfiguration hostReallocationConfig,
            Metrics metrics,
            DeploymentConfiguration deploymentConfiguration,
            AdminTracer adminTracer,
            Shutdown shutdown,
            AlertProxyHttpClient alertProxyHttpClient
    ) {
        return new PlanRequestHandler(
                plannerDataAccessClient,
                maskedErrorMessage,
                resourceRole,
                securityEnabled,
                hostReallocationConfig,
                pbsMaxIdlePeriodInSeconds,
                metrics,
                isAlgoTest(deploymentConfiguration),
                adminTracer,
                shutdown,
                alertProxyHttpClient,
                new Random());
    }

    @Bean
    PbsRegistrationHandler pbsRegistrationHandler(
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            @Value("${error.message}") String maskedErrorMessage,
            Metrics metrics,
            @Value("${server-api-roles.registration}") String resourceRole,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            DeploymentConfiguration deploymentConfiguration,
            AdminTracer adminTracer,
            Shutdown shutdown
    ) {
        return new PbsRegistrationHandler(plannerDataAccessClient,
                maskedErrorMessage,
                resourceRole,
                securityEnabled,
                metrics,
                isAlgoTest(deploymentConfiguration),
                adminTracer,
                shutdown
        );
    }

    @Bean
    PbsHealthHandler pbsHealthHandler(
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            @Value("${services.pbs-max-idle-period-sec}") int pbsMaxIdlePeriodInSeconds,
            @Value("${error.message}") String maskedErrorMessage,
            Metrics metrics,
            @Value("${server-api-roles.pbs-health}") String resourceRole,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            AlertProxyHttpClient alertProxyHttpClient,
            Shutdown shutdown) {
        return new PbsHealthHandler(plannerDataAccessClient,
                pbsMaxIdlePeriodInSeconds,
                maskedErrorMessage,
                metrics,
                resourceRole,
                securityEnabled,
                alertProxyHttpClient,
                shutdown);
    }

    @Bean
    LineItemsTokensSummaryHandler lineItemsTokensSummaryHandler(
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            CsvMapperFactory csvMapperFactory,
            @Value("${server-api-roles.tracer}") String resourceRole,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            Metrics metrics,
            AlertProxyHttpClient alertHttpClient,
            Shutdown shutdown) {
        return new LineItemsTokensSummaryHandler(
                plannerDataAccessClient,
                csvMapperFactory,
                resourceRole,
                securityEnabled,
                metrics,
                alertHttpClient,
                shutdown);
    }

    @Bean
    HostAllocationAlgoTestHandler hostAllocationAlgoTestHandler(
            DeliveryDataServiceAlgoTest deliveryDataServiceAlgoTest,
            HostAllocationServiceAlgoTest hostAllocationServiceAlgoTest) {
        return new HostAllocationAlgoTestHandler(deliveryDataServiceAlgoTest, hostAllocationServiceAlgoTest);
    }

    @Bean
    PlannerAdaptersUpdateAlgoTestHandler adaptersUpdateAlgoTestHandler(PlannerAdapterServicesAlgoTest adapterServices) {
        return new PlannerAdaptersUpdateAlgoTestHandler(adapterServices);
    }

    @Bean
    PrepShutdownHandler prepShutdownHandler(
            Shutdown shutdown,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            @Value("${server-api-roles.tracer}") String resourceRole
    ) {
        return new PrepShutdownHandler(shutdown, resourceRole, securityEnabled);
    }

    @Bean
    CeaseShutdownHandler ceaseShutdownHandler(
            Shutdown shutdown,
            @Value("${server-auth.authentication-enabled}") boolean securityEnabled,
            @Value("${server-api-roles.tracer}") String resourceRole
    ) {
        return new CeaseShutdownHandler(shutdown, resourceRole, securityEnabled);
    }

    private boolean isAlgoTest(DeploymentConfiguration config) {
        return "algotest".equals(config.getProfile());
    }

    @Bean
    public CsvMapperFactory csvMapperFactory() {
        new CsvMapper().findAndRegisterModules();
        return new CsvMapperFactory();
    }

    public static class CsvMapperFactory {

        public CsvMapper getCsvMapper() {
            CsvMapper csvMapper = new CsvMapper();
            csvMapper.findAndRegisterModules();
            csvMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            csvMapper.enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING);
            return csvMapper;
        }

    }

}
