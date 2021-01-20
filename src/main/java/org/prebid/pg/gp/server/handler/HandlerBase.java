package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.ext.web.RoutingContext;
import org.prebid.pg.gp.server.exception.InvalidRequestException;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.beans.factory.annotation.Value;

import java.util.Objects;

/**
 * A handler base class to handle HTTP request.
 */
public abstract class HandlerBase implements Handler<RoutingContext> {

    protected final String resourceRole;

    protected final boolean securityEnabled;

    protected final String maskedErrorMessage;

    protected final Metrics metrics;

    protected final Shutdown shutdown;

    protected final AlertProxyHttpClient alertHttpClient;

    public HandlerBase(
            String resourceRole,
            boolean securityEnabled,
            @Value("${error.message}") String maskedErrorMessage,
            Metrics metrics,
            AlertProxyHttpClient alertHttpClient,
            Shutdown shutDown) {
        this.resourceRole = resourceRole;
        this.securityEnabled = securityEnabled;
        this.maskedErrorMessage = Objects.requireNonNull(maskedErrorMessage);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
        this.shutdown = shutDown;
        this.metrics = Objects.requireNonNull(metrics);
        if (securityEnabled) {
            logger().info("{0} protected by role {1}", handlerName(), resourceRole);
        }
    }

    /**
     * Handles the http request and send response back.
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

        if (securityEnabled) {
            logger().info("{0}::secure::{1}", handlerName(), routingContext.user());
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
            logger().warn("{0} is insecure", handlerName());
            processRequest(routingContext);
        }
    }

    void handleErrorResponse(HttpServerResponse response, Throwable exception) {
        metrics.incCounter(metricName("exc"));
        if (exception instanceof InvalidRequestException) {
            response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
            return;
        }
        alertHttpClient.raiseEvent(
                Constants.GP_PLANNER_LINE_ITEM_TOKENS_SUMMARY_HANDLER_ERROR,
                AlertPriority.MEDIUM,
                String.format("Exception in %s::%s", handlerName(), exception.getMessage()));
        response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
    }

    protected abstract void processRequest(RoutingContext routingContext);

    protected abstract Logger logger();

    protected abstract String handlerName();

    protected abstract String metricName(String tag);

}
