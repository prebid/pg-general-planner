package org.prebid.pg.gp.server.handler.algotest;

import org.prebid.pg.gp.server.services.algotest.DeliveryDataServiceAlgoTest;
import org.prebid.pg.gp.server.services.algotest.HostAllocationServiceAlgoTest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.springframework.util.StringUtils;

import java.time.Instant;

/**
 * A handler for the request of refresh line item delivery stats data and run host-based
 * token reallocation when executing in the simulation environment.
 */
public class HostAllocationAlgoTestHandler implements Handler<RoutingContext> {
    private static final Logger logger = LoggerFactory.getLogger(HostAllocationAlgoTestHandler.class);

    private static final String PG_SIM_TIMESTAMP_HEADER = "pg-sim-timestamp";

    private final DeliveryDataServiceAlgoTest deliveryDataServiceAlgoTest;
    private final HostAllocationServiceAlgoTest hostAllocationServiceAlgoTest;

    public HostAllocationAlgoTestHandler(DeliveryDataServiceAlgoTest deliveryDataServiceAlgoTest,
            HostAllocationServiceAlgoTest hostAllocationServiceAlgoTest) {
        this.deliveryDataServiceAlgoTest = deliveryDataServiceAlgoTest;
        this.hostAllocationServiceAlgoTest = hostAllocationServiceAlgoTest;
    }

    @Override
    public void handle(RoutingContext routingContext) {
        logger.info("start algotest ...");

        String simTimeStr = routingContext.request().getHeader(PG_SIM_TIMESTAMP_HEADER);
        if (StringUtils.isEmpty(simTimeStr)) {
            logger.error("algotest updatePlan request missing {} header.", PG_SIM_TIMESTAMP_HEADER);
            routingContext.response().setStatusCode(400).end();
            return;
        }

        logger.info("Received sim time: {0}", simTimeStr);

        deliveryDataServiceAlgoTest.doControlledRefreshDeliveryData(simTimeStr)
                .setHandler(asr -> {
                    if (asr.succeeded()) {
                        hostAllocationServiceAlgoTest.doControlledCalculate(Instant.parse(simTimeStr))
                                .setHandler(rs -> {
                                    if (rs.succeeded()) {
                                        logger.info("algotest successfully done.");
                                    } else {
                                        logger.error("algotest error while host reallocation.", rs.cause());
                                    }
                                    handleResponse(routingContext, rs);
                                });
                    } else {
                        logger.error("algotest failed while refreshing stats data.");
                        handleResponse(routingContext, asr);
                    }
                });
    }

    private void handleResponse(RoutingContext routingContext, AsyncResult<?> ars) {
        if (ars.succeeded()) {
            routingContext.response().setStatusCode(HttpResponseStatus.OK.code()).end();
        } else {
            routingContext.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end();
        }
    }
}
