package org.prebid.pg.gp.server.handler;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.Shutdown;
import org.springframework.beans.factory.annotation.Value;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A handler for PBS server health status checking request.
 */
public class PbsHealthHandler extends HandlerBase {
    private static final Logger logger = LoggerFactory.getLogger(PbsHealthHandler.class);

    private final CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient;

    private final int pbsMaxIdlePeriodInSeconds;

    public PbsHealthHandler(
            CircuitBreakerSecuredPlannerDataAccessClient dataAccessClient,
            int pbsMaxIdlePeriodInSeconds,
            @Value("${error.message}") String maskedErrorMessage,
            Metrics metrics,
            String resourceRole,
            boolean securityEnabled,
            AlertProxyHttpClient alertHttpClient,
            Shutdown shutDown) {
        super(resourceRole, securityEnabled, maskedErrorMessage, metrics, alertHttpClient, shutDown);
        this.dataAccessClient = Objects.requireNonNull(dataAccessClient);
        this.pbsMaxIdlePeriodInSeconds = pbsMaxIdlePeriodInSeconds;
    }

    @SuppressWarnings({"squid:S1854", "squid:S1481"})
    @Override
    protected void processRequest(RoutingContext routingContext) {
        final long start = System.currentTimeMillis();
        metrics.incCounter(metricName("requests-served"));
        parseRequest(routingContext)
                .compose(req -> {
                    Instant activeSince = Instant.now().minusSeconds(pbsMaxIdlePeriodInSeconds);
                    return dataAccessClient.findRegistrations(
                            activeSince, req.getVendor(), req.getRegion(), req.getInstanceId());
                })
                .setHandler(ar -> finalHandler(ar, routingContext, start));
    }

    private Future<Request> parseRequest(RoutingContext routingContext) {
        final MultiMap params = routingContext.request().params();
        Request req = params == null
                ? Request.builder().build()
                : Request.builder()
                        .vendor(params.get("vendor"))
                        .region(params.get("region"))
                        .instanceId(params.get("instanceId"))
                        .build();
        logger.info("PbsHealthHandler::Request{0}", req);
        return Future.succeededFuture(req);
    }

    private void finalHandler(AsyncResult<List<Map<String, Object>>> asyncResult,
            RoutingContext routingContext, long startTime) {
        HttpServerResponse response = routingContext.response();
        response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);

        if (asyncResult.succeeded()) {
            logger.info("result size:{0}", asyncResult.result().size());
            try {
                String jsonResp = Json.encode(asyncResult.result());
                response.setStatusCode(HttpResponseStatus.OK.code()).end(jsonResp);
            } catch (Exception ex) {
                handleErrorResponse(response, ex);
            }
        } else {
            handleErrorResponse(response, asyncResult.cause());
        }
        metrics.updateTimer(metricName("processing-time"), System.currentTimeMillis() - startTime);
    }

    @Override
    protected String metricName(String tag) {
        return String.format("pbs-health-request.%s", tag);
    }

    @Override
    protected String handlerName() {
        return "PbsHealthHandler";
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    @Getter
    @Builder
    @ToString
    static class Request {
        private String vendor;

        private String region;

        private String instanceId;
    }

}
