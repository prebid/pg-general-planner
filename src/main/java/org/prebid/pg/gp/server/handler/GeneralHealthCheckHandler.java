package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.prebid.pg.gp.server.model.Shutdown;

import java.util.Objects;

/**
 * A handler for general health check request.
 */
public class GeneralHealthCheckHandler implements Handler<RoutingContext> {

    private final Shutdown shutdown;

    private static final String MESSAGE =
            "{\n"
            + "  \"application\": {\n"
            + "    \"status\": \"%s\"\n"
            + "  }\n"
            + "}";

    public GeneralHealthCheckHandler(Shutdown shutdown) {
        this.shutdown = Objects.requireNonNull(shutdown);
    }

    /**
     * Handles general health check request.
     *
     * @param routingContext context of request and response
     */
    @Override
    public void handle(RoutingContext routingContext) {
        if (shutdown.getInitiating() == Boolean.TRUE) {
            routingContext.response()
                    .setStatusCode(HttpResponseStatus.BAD_GATEWAY.code())
                    .end(String.format(MESSAGE, "shutting down"));
            return;
        }
        routingContext.response()
                .setStatusCode(HttpResponseStatus.OK.code())
                .end(String.format(MESSAGE, "ready"));
    }

}

