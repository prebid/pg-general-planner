package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.model.AdminPayload;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * A handler for administration HTTP request.
 */
public class AdminHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(AdminHandler.class);

    private final AdminTracer tracer;

    private final String resourceRole;

    private final boolean securityEnabled;

    private final Shutdown shutdown;

    private final int maxDurationInSeconds;

    private final int pbsMaxIdlePeriodInSeconds;

    private final int batchSize;

    private final List<String> applications = new ArrayList<>();

    private CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient;

    public AdminHandler(
            AdminTracer adminTracer,
            int maxDurationInSeconds,
            int pbsMaxIdlePeriodInSeconds,
            String applicationListStr,
            int batchSize,
            CircuitBreakerSecuredPlannerDataAccessClient plannerDataAccessClient,
            String resourceRole,
            boolean securityEnabled,
            Shutdown shutdown
    ) {
        this.resourceRole = resourceRole;
        this.securityEnabled = securityEnabled;
        this.tracer = adminTracer;
        this.shutdown = shutdown;
        this.maxDurationInSeconds = maxDurationInSeconds;
        this.pbsMaxIdlePeriodInSeconds = pbsMaxIdlePeriodInSeconds;
        this.batchSize = batchSize;
        this.plannerDataAccessClient = Objects.requireNonNull(plannerDataAccessClient);
        Collections.addAll(this.applications, applicationListStr.split(","));
        if (securityEnabled) {
            logger.info("AdminHandler protected by role {0}", resourceRole);
        }
    }

    /**
     * Handles administration HTTP request.
     *
     * @param routingContext context for http request and response
     */
    @Override
    public void handle(RoutingContext routingContext) {
        if (securityEnabled) {
            logger.info("secure::" + routingContext.user());
            if (routingContext.user() == null) {
                HttpServerResponse response = routingContext.response();
                response.setStatusCode(HttpResponseStatus.FORBIDDEN.code()).end();
                return;
            }
            routingContext.user().isAuthorized(resourceRole, rs -> {
                if (rs.succeeded() && rs.result().booleanValue()) {
                    processRequest(routingContext);
                } else {
                    HttpServerResponse response = routingContext.response();
                    response.setStatusCode(HttpResponseStatus.FORBIDDEN.code()).end();
                }
            });
        } else {
            logger.warn("AdminHandler is insecure");
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

        final Buffer body = routingContext.getBody();
        if (body == null || body.toString().isEmpty()) {
            routingContext.response()
                    .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
                    .end("Empty admin control request");
            return;
        }

        logger.info("Admin Control Request String: {0}", body.toString().replaceAll(System.lineSeparator(), " "));

        AdminPayload adminPayload0 = null;
        try {
            adminPayload0 = Json.decodeValue(body, AdminPayload.class);
        } catch (DecodeException e) {
            String msg = String.format("Cannot decode incoming Admin Control Request::%s", e.getMessage());
            logger.error(msg);
            routingContext.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(msg);
            return;
        }

        final AdminPayload adminPayload = adminPayload0;
        adminPayload.setExpiresAt(Instant.now().plus(Duration.ofMinutes(adminPayload.getExpiresInMinutes())));
        logger.info("Admin Control Request object: {0}", adminPayload);

        String errors = null;
        if (adminPayload.getApp() == null) {
            errors = "Empty [app] field in admin control request";
        } else if (!applications.stream().map(String::trim).anyMatch(adminPayload.getApp()::equals)) {
            errors = adminPayload.getApp() + " is not a supported [app]";
        } else if (adminPayload.getServices() == null
                && adminPayload.getTracer() == null
                && adminPayload.getStoredRequest() == null
                && adminPayload.getStoredRequestAmp() == null) {
            errors = "Empty directive in admin control request";
        }
        if (!StringUtils.isEmpty(errors)) {
            routingContext.response()
                    .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
                    .end(errors);
            return;
        }

        if (Constants.APP_GP.equalsIgnoreCase(adminPayload.getApp())) {
            if (adminPayload.getTracer() != null) {
                logger.info("GP Admin Control Request");
                tracer.setTracer(adminPayload.getTracer(), maxDurationInSeconds);
                logger.info("Updated GP Tracer=" + tracer);
                routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end();
            }
        } else {
            plannerDataAccessClient
                    .findActiveHosts(Instant.now().minusSeconds(pbsMaxIdlePeriodInSeconds))
                    .setHandler(arPbsHosts -> {
                        if (arPbsHosts.succeeded()) {
                            handleActiveHosts(routingContext, adminPayload, arPbsHosts.result());
                        } else {
                            logger.error("Admin command processing error", arPbsHosts.cause().getMessage());
                            routingContext.response()
                                    .setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
                        }
                    });
        }
    }

    private void handleActiveHosts(RoutingContext routingContext, AdminPayload adminPayload, List<PbsHost> pbsHosts) {
        List<AdminEvent> adminEvents = new ArrayList<>();
        Instant createdAt = Instant.now();
        AdminEvent.Directive payload = AdminEvent.Directive.builder()
                .services(adminPayload.getServices())
                .tracer(adminPayload.getTracer())
                .storedRequestAmp(adminPayload.getStoredRequestAmp())
                .storedRequest(adminPayload.getStoredRequest())
                .build();
        for (PbsHost pbsHost : pbsHosts) {
            logger.info(pbsHost);
            if (!adminPayload.getVendor().equalsIgnoreCase(pbsHost.getVendor())) {
                logger.info("admin event vendor {0} does not match for {1}, skip.",
                        adminPayload.getVendor(), pbsHost);
            } else if (!Arrays.stream(adminPayload.getRegions().toArray(new String[adminPayload.getRegions().size()]))
                    .anyMatch(pbsHost.getRegion()::equalsIgnoreCase)) {
                logger.info("admin event regions {0} do not match for {1}, skip.",
                        adminPayload.getRegions(), pbsHost);
            } else {
                adminEvents.add(AdminEvent.builder()
                        .id(UUID.randomUUID().toString())
                        .app(Constants.APP_PBS)
                        .vendor(pbsHost.getVendor())
                        .region(pbsHost.getRegion())
                        .instanceId(pbsHost.getHostInstanceId())
                        .directive(payload)
                        .expiryAt(adminPayload.getExpiresAt())
                        .createdAt(createdAt)
                        .build());
            }
        }
        logger.info("Admin Events={0},", adminEvents);
        plannerDataAccessClient.updateAdminEvents(adminEvents, batchSize)
                .setHandler(async -> {
                    HttpResponseStatus status = async.succeeded()
                            ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR;
                    routingContext.response()
                                .setStatusCode(status.code()).end();
                });
    }

}
