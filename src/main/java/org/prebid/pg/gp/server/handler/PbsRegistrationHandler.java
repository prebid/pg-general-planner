package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.prebid.pg.gp.server.exception.InvalidRequestException;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.util.Constants;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A handler for registration request from PBS servers.
 */
public class PbsRegistrationHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(PbsRegistrationHandler.class);

    private String resourceRole;

    private final Metrics metrics;

    private final boolean isAlgoTest;

    private final AdminTracer tracer;

    private final Shutdown shutdown;

    private String maskedErrorMessage;

    private final boolean securityEnabled;

    private CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient;

    static final String REG_REQUEST_KEY = "registration";

    public PbsRegistrationHandler(
            CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient,
            String maskedErrorMessage,
            String resourceRole,
            boolean securityEnabled,
            Metrics metrics,
            boolean isAlgoTest,
            AdminTracer adminTracer,
            Shutdown shutdown
    ) {
        this.dataAccessClient = dataAccessClient;
        this.maskedErrorMessage = maskedErrorMessage;
        this.resourceRole = resourceRole;
        this.metrics = metrics;
        this.securityEnabled = securityEnabled;
        this.isAlgoTest = isAlgoTest;
        this.tracer = adminTracer;
        this.shutdown = shutdown;
        if (securityEnabled && !isAlgoTest) {
            logger.info("PbsRegistrationHandler protected by role {0}", resourceRole);
        }
    }

    /**
     * Handles registration request from PBS servers.
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
            routingContext.user().isAuthorized(resourceRole, r -> {
                if (r.succeeded() && r.result().booleanValue()) {
                    processRequest(routingContext);
                } else {
                    routingContext.response().setStatusCode(HttpResponseStatus.FORBIDDEN.code()).end();
                }
            });
        } else {
            processRequest(routingContext);
        }
    }

    private void processRequest(RoutingContext routingContext) {
        final long start = System.currentTimeMillis();

        parseRequest(routingContext)
                .compose(registration -> dataAccessClient.updateRegistration(registration))
                .compose(rs -> includeAdminDirectiveIfAny(routingContext.get(REG_REQUEST_KEY)))
                .setHandler(ar -> finalHandler(ar, routingContext, start));
    }

    private Future<Registration> parseRequest(RoutingContext routingContext) {
        final Buffer body = routingContext.getBody();

        if (!tracer.checkActive()) {
            logger.debug("Registration request: {0}", body);
        }

        if (body == null) {
            logger.error("Incoming registration request has no body");
            return Future.failedFuture(
                    new InvalidRequestException(GPConstants.BAD_FORMAT, "Incoming request has no body"));
        }

        try {
            final Registration registration = Json.decodeValue(body, Registration.class);
            List<String> errors = registration.validate();
            if (!errors.isEmpty()) {
                logger.error("Cannot decode incoming health report::Missing fields/improper values::" + errors);
                return Future.failedFuture(
                        new InvalidRequestException(
                                GPConstants.BAD_FORMAT,
                                String.format(
                                        "Cannot decode incoming health report::Missing fields/improper values::%s",
                                        errors)));
            }
            if (tracer.checkActive()) {
                if (tracer.matchVendor(registration.getVendor()) && tracer.matchRegion(registration.getRegion())) {
                    logger.info("{0}::{1}", GPConstants.TRACER, registration);
                }
            } else {
                logger.info(
                        "Registration request from {0}",
                        String.format(
                                "%s|%s|%s",
                                registration.getVendor(),
                                registration.getRegion(),
                                registration.getInstanceId()
                        )
                );
            }
            routingContext.put(REG_REQUEST_KEY, registration);
            return Future.succeededFuture(registration);
        } catch (DecodeException ex) {
            String msg = String.format("Cannot decode incoming health report::%s", ex.getMessage());
            logger.warn(msg);
            return Future.failedFuture(new InvalidRequestException(GPConstants.BAD_FORMAT, msg));
        }
    }

    private void finalHandler(AsyncResult<AdminEvent> asyncResult, RoutingContext routingContext,
            long startTime) {
        HttpServerResponse response = routingContext.response();
        int statusCode = HttpResponseStatus.OK.code();

        List<String> errorDetails = new ArrayList<>();

        if (asyncResult.failed()) {
            logger.error("Failure in processing PBS registration report::{0}", asyncResult.cause().getMessage());
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
            }
            if (statusCode == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()) {
                errorDetails.add(maskedErrorMessage);
            }
        }

        response.setStatusCode(statusCode).putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        if (errorDetails.isEmpty()) {
            if (asyncResult.result() != null) {
                AdminEvent adminEvent = asyncResult.result();
                AdminTracer adminTracer = adminEvent.getDirective().getTracer();
                if (adminTracer != null) {
                    adminTracer.setEnabled(null);
                    adminTracer.setExpiresAt(null);
                }
                response.end(Json.encode(adminEvent.getDirective()));
                dataAccessClient.deleteAdminEvent(adminEvent.getId());
            } else {
                response.end();
            }
        } else {
            metrics.incCounter(metricName("exc"));
            response.end(Json.encode(errorDetails));
        }
        metrics.updateTimer(metricName("processing-time"), System.currentTimeMillis() - startTime);
    }

    private Future<AdminEvent> includeAdminDirectiveIfAny(Registration registration) {
        if (registration == null) {
            return Future.succeededFuture();
        }
        return dataAccessClient.findEarliestActiveAdminEvent(Constants.APP_PBS, registration, Instant.now());
    }

    private String metricName(String tag) {
        return String.format("pbs-registration.%s", tag);
    }
}
