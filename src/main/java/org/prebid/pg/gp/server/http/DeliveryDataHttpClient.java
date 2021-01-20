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
import org.prebid.pg.gp.server.spring.config.app.DeliveryDataConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;
import org.prebid.pg.gp.server.util.Constants;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * A http client to retrieve line item delivery statistics from stats server.
 */
public class DeliveryDataHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(DeliveryDataHttpClient.class);

    private final HttpClient httpClient;

    private final AdminTracer tracer;

    private final Metrics metrics;

    private final AlertProxyHttpClient alertHttpClient;

    private final DeliveryDataConfiguration deliveryDataConfiguration;

    public DeliveryDataHttpClient(
            Vertx vertx,
            HttpClientConfiguration httpClientConfiguration,
            DeliveryDataConfiguration deliveryDataConfiguration,
            Metrics metrics,
            AdminTracer adminTracer,
            AlertProxyHttpClient alertHttpClient
    ) {
        final HttpClientOptions options = new HttpClientOptions()
                .setMaxPoolSize(httpClientConfiguration.getMaxPoolSize())
                .setTryUseCompression(true)
                .setConnectTimeout(httpClientConfiguration.getConnectTimeoutSec() * 1000);
        this.deliveryDataConfiguration = Objects.requireNonNull(deliveryDataConfiguration);
        this.httpClient = vertx.createHttpClient(options);
        this.metrics = Objects.requireNonNull(metrics);
        this.tracer = Objects.requireNonNull(adminTracer);
        this.alertHttpClient = Objects.requireNonNull(alertHttpClient);
    }

    /**
     * Sends http request to the stats server to retrieve latest line item delivery stats information.
     *
     * @param method the http method of the request
     * @param url the stats server resource url
     * @param username the username
     * @param password the password
     * @param simTime the current time of the simulation environment.
     * @return a future of {@link HttpResponseContainer}
     */
    public Future<HttpResponseContainer> request(HttpMethod method, String url, String username, String password,
            String simTime) {
        if (StringUtils.isEmpty(url)) {
            return Future.failedFuture("DelStats URL is empty and must be populated");
        }

        final Future<HttpResponseContainer> future = Future.future();

        String uuid = UUID.randomUUID().toString();
        if (tracer.checkActive()) {
            logger.info("{0}::{1}::{2}", GPConstants.TRACER, url, uuid);
        }

        long start = System.currentTimeMillis();

        final HttpClientRequest httpClientRequest = httpClient.requestAbs(method, url)
                        .putHeader("Authorization", HttpUtil.generateBasicAuthHeaderEntry(username, password))
                        .putHeader("pg-trx-id", uuid)
                        .setTimeout((long) deliveryDataConfiguration.getTimeoutSec() * 1000)
                        .handler(response -> {
                            metrics.updateTimer(
                                    metricName("data-request.response-time"),
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
        if (tracer.checkActiveAndRaw()) {
            logger.info("{0}::{1}::{2}", GPConstants.TRACER, url, body);
        }
        future.tryComplete(HttpResponseContainer.of(response.statusCode(), response.headers(), body));
    }

    private void handleExceptionResponse(Throwable exception, String url, Future<HttpResponseContainer> future) {
        String msg = String.format(
                "Error occurred during request to DelStats service at %s::%s", url, exception.toString());
        logger.error(msg);
        if (exception instanceof TimeoutException) {
            metrics.incCounter(metricName("exc.timeout"));
        } else {
            metrics.incCounter(metricName("exc.others"));
        }
        alertHttpClient.raiseEvent(Constants.GP_PLANNER_DEL_STATS_CLIENT_ERROR, AlertPriority.HIGH, msg);
        future.tryFail(exception);
    }

    private String metricName(String tag) {
        return String.format("delivery-data-adapter.%s", tag);
    }

}
