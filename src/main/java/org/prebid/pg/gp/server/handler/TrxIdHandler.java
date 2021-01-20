package org.prebid.pg.gp.server.handler;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * A handler to copy {@code pg-trx-id} header from request to response.
 */
public class TrxIdHandler implements Handler<RoutingContext> {

    static final String TRX_ID = "pg-trx-id";

    /**
     * Copies {@code pg-trx-id} header from request to response.
     *
     * @param context a request and response context
     */
    @Override
    public void handle(RoutingContext context) {
        String trxId = context.request().headers().get(TRX_ID);

        if (trxId != null) {
            context.response().headers().add(TRX_ID, trxId);
        }
        context.next();
    }
}
