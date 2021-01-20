package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.prebid.pg.gp.server.model.Shutdown;

/**
 * A handler for server shutdown notification request.
 */
public class PrepShutdownHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(PrepShutdownHandler.class);

    private final String resourceRole;

    private final boolean securityEnabled;

    private final Shutdown shutdown;

    public PrepShutdownHandler(
            Shutdown shutdown,
            String resourceRole,
            boolean securityEnabled
    ) {
        this.shutdown = shutdown;
        this.resourceRole = resourceRole;
        this.securityEnabled = securityEnabled;
        if (securityEnabled) {
            logger.info("PrepShutdownHandler protected by role {0}", resourceRole);
        }
    }

    /**
     * Handles server shutdown notification request.
     *
     * @param routingContext context of request and response
     */
    @Override
    public void handle(RoutingContext routingContext) {
        if (securityEnabled) {
            logger.info("PrepShutdownHandler::secure::" + routingContext.user());
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
            logger.warn("PrepShutdownHandler is insecure");
            processRequest(routingContext);
        }
    }

    private void processRequest(RoutingContext routingContext) {
        shutdown.setInitiating(Boolean.TRUE);
        logger.info("Server is preparing for shutdown");
        routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end();
    }
}
