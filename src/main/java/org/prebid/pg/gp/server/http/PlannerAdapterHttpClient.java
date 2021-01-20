package org.prebid.pg.gp.server.http;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations.PlannerAdapterConfiguration;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * A http client to retrieve line item information from Planner Adapters.
 */
public class PlannerAdapterHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(PlannerAdapterHttpClient.class);

    private final HttpClient httpClient;

    private final Metrics metrics;

    private final AdminTracer tracer;

    private final PlannerAdapterConfiguration adapterConfiguration;

    private final AlertProxyHttpClient alertHttpClient;

    public PlannerAdapterHttpClient(
            Vertx vertx,
            HttpClientConfiguration httpClientConfiguration,
            PlannerAdapterConfiguration adapterConfiguration,
            Metrics metrics,
            AdminTracer adminTracer,
            AlertProxyHttpClient alertHttpClient
    ) {

        final HttpClientOptions options = new HttpClientOptions()
                .setMaxPoolSize(httpClientConfiguration.getMaxPoolSize())
                .setTryUseCompression(true)
                .setConnectTimeout(httpClientConfiguration.getConnectTimeoutSec() * 1000);
        this.httpClient = vertx.createHttpClient(options);
        this.metrics = Objects.requireNonNull(metrics);
        this.tracer = Objects.requireNonNull(adminTracer);
        this.adapterConfiguration = Objects.requireNonNull(adapterConfiguration);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
    }

    /**
     * Sends http request to the planner adapter to retrieve line item delivery plans.
     *
     * @param method the http method of the request
     * @param url the planner adapter resource url
     * @param username the username
     * @param password the password
     * @param simTime the current time of the simulation environment
     * @return a future of {@link HttpResponseContainer}
     */
    public Future<HttpResponseContainer> request(HttpMethod method, String url, String username, String password,
            String simTime) {
        logger.info("Making request to Planner Adapter at {0}", url);
        if (StringUtils.isEmpty(url)) {
            return Future.failedFuture("PlannerAdapter URL is empty and must be populated");
        }

        final Future<HttpResponseContainer> future = Future.future();
        final long start = System.currentTimeMillis();

        final String uuid = UUID.randomUUID().toString();
        if (tracer.checkActive()) {
            logger.info("{0}::{1}::{2}", GPConstants.TRACER, url, uuid);
        }

        final HttpClientRequest httpClientRequest = httpClient.requestAbs(method, url)
                .putHeader("Authorization", HttpUtil.generateBasicAuthHeaderEntry(username, password))
                .putHeader("pg-trx-id", UUID.randomUUID().toString())
                .setTimeout((long) adapterConfiguration.getTimeoutSec() * 1000)
                .handler(response -> {
                    metrics.updateTimer(
                            metricName("response-time"),
                            System.currentTimeMillis() - start
                    );
                    handleResponse(response, url, future);
                })
                .exceptionHandler(exception -> handleExceptionResponse(exception, url, future));
        if (!StringUtils.isEmpty(simTime)) {
            httpClientRequest.putHeader("pg-sim-timestamp", simTime);
        }

        httpClientRequest.end();

        return future;
    }

    private void handleResponse(HttpClientResponse response, String url, Future<HttpResponseContainer> future) {
        response
                .bodyHandler(buffer -> successResponse(url, buffer.toString(), response, future))
                .exceptionHandler(exception -> handleExceptionResponse(exception, url, future));
    }

    private void successResponse(
            String url, String body, HttpClientResponse response, Future<HttpResponseContainer> future
    ) {
        logger.debug("URL={0}, Response body={1}", url, body);
        if (tracer.checkActiveAndRaw()) {
            logger.info("{0}::{1}::{2}", GPConstants.TRACER, url, body);
        }
        future.tryComplete(HttpResponseContainer.of(response.statusCode(), response.headers(), body));
    }

    private void handleExceptionResponse(Throwable exception, String url, Future<HttpResponseContainer> future) {
        String msg = String.format(
                "Error occurred during request to planner adapter at %s::%s", url, exception.toString());
        logger.error(msg);
        if (exception instanceof TimeoutException) {
            metrics.incCounter(metricName("exc.timeout"));
        } else {
            metrics.incCounter(metricName("exc.others"));
        }
        alertHttpClient.raiseEvent(Constants.GP_PLANNER_ADAPTER_CLIENT_ERROR, AlertPriority.HIGH, msg);
        future.tryFail(exception);
    }

    private String metricName(String tag) {
        return String.format("planner-adapter.%s.line-item-request.%s", adapterConfiguration.getName(), tag);
    }
}
