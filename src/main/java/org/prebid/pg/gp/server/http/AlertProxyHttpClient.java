package org.prebid.pg.gp.server.http;

import com.mysql.cj.core.util.StringUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.pg.gp.server.model.AlertEvent;
import org.prebid.pg.gp.server.model.AlertPayload;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.model.AlertSource;
import org.prebid.pg.gp.server.model.GPConstants;
import org.prebid.pg.gp.server.model.HttpResponseContainer;
import org.prebid.pg.gp.server.spring.config.app.AlertProxyConfiguration;
import org.prebid.pg.gp.server.spring.config.app.AlertProxyConfiguration.AlertPolicy;
import org.prebid.pg.gp.server.spring.config.app.DeploymentConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A http client to send alerts to the remote alert administration server.
 */
public class AlertProxyHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(AlertProxyHttpClient.class);

    private final HttpClient httpClient;

    private final DeploymentConfiguration deploymentConfiguration;

    private final AlertProxyConfiguration alertProxyConfiguration;

    private final Map<String, AlertPolicy> alertPolicies;

    private final AlertPolicy defaultAlertPolicy;

    private final ConcurrentMap<AlertEvent, AtomicInteger> alertCache;

    public AlertProxyHttpClient(
            Vertx vertx,
            HttpClientConfiguration httpClientConfiguration,
            DeploymentConfiguration deploymentConfiguration,
            AlertProxyConfiguration alertProxyConfiguration,
            ConcurrentMap<AlertEvent, AtomicInteger> alertCache
    ) {
        final HttpClientOptions options = new HttpClientOptions()
                .setMaxPoolSize(httpClientConfiguration.getMaxPoolSize())
                .setConnectTimeout(httpClientConfiguration.getConnectTimeoutSec() * 1000);
        this.httpClient = vertx.createHttpClient(options);
        this.deploymentConfiguration = Objects.requireNonNull(deploymentConfiguration);
        this.alertProxyConfiguration = Objects.requireNonNull(alertProxyConfiguration);
        this.alertPolicies = Objects.requireNonNull(alertProxyConfiguration.getPolicies()).stream()
                .collect(Collectors.toMap(AlertPolicy::getAlertName, it -> it));
        this.defaultAlertPolicy = Objects.requireNonNull(alertPolicies.get("default"));
        this.alertCache = Objects.requireNonNull(alertCache);
    }

    /**
     * Raises the alert event with the given information.
     *
     * @param name name of the alert
     * @param priority priority of the alert
     * @param details details of the alert
     * @return a future to indicate if the alert event has be successfully raised
     */
    public Future<Void> raiseEvent(String name, AlertPriority priority, String details) {
        final List events = new ArrayList<AlertEvent>();
        AlertEvent event = event(GPConstants.RAISE, priority, name, details, source());
        events.add(event);
        if (shouldFireAlert(event)) {
            logger.info(Json.encode(payload(events)));
            request(HttpMethod.POST, Json.encode(payload(events)));
        }
        return Future.succeededFuture();
    }

    public Future<Void> clearAlert(AlertEvent alert) {
        if (alertCache.containsKey(alert)) {
            alertCache.remove(alert);
            final List events = new ArrayList<AlertEvent>();
            events.add(alert);
            logger.info("Clear alert:{0}", alert);
            request(HttpMethod.POST, Json.encode(payload(events)));
        }
        return Future.succeededFuture();
    }

    boolean shouldFireAlert(AlertEvent alert) {
        AlertPolicy policy = alertPolicies.getOrDefault(alert, defaultAlertPolicy);
        AtomicInteger countRef = alertCache.computeIfAbsent(alert, key -> new AtomicInteger(0));
        logger.debug("alert::{0}", alert.toString());
        logger.debug("alertCache::{0}", alertCache.toString());
        int count = countRef.addAndGet(1);
        if (count <= policy.getInitialAlerts()) {
            logger.debug("Initial firings::count::alert::{0}::{1}", count, alert.toString());
            return true;
        }
        int remainder = (count - policy.getInitialAlerts()) % policy.getAlertFrequency();
        if (logger.isDebugEnabled() && remainder == 0) {
            logger.debug("Subsequent firings:count::alert::{0}::{1}", count, alert.toString());
        } else if (logger.isDebugEnabled() && remainder != 0) {
            logger.debug("Throttled firings::count::alert::{0}::{1}", count, alert.toString());
        }
        return remainder == 0;
    }

    private AlertPayload payload(List<AlertEvent> events) {
        return AlertPayload.builder().events(events).build();
    }

    private AlertSource source() {
        return AlertSource.builder()
                .env(deploymentConfiguration.getProfile())
                .hostId("FARGATE_TASK")
                .region(deploymentConfiguration.getRegion())
                .dataCenter(deploymentConfiguration.getDataCenter())
                .subSystem(deploymentConfiguration.getSubSystem())
                .system(deploymentConfiguration.getSystem())
                .build();
    }

    private AlertEvent event(String action, AlertPriority priority, String name, String details,
            AlertSource alertSource) {
        return AlertEvent.builder()
                .id(UUID.randomUUID().toString())
                .action(action.toUpperCase())
                .priority(priority)
                .name(name)
                .details(details)
                .updatedAt(Instant.now())
                .source(alertSource)
                .build();
    }

    public Future<HttpResponseContainer> request(HttpMethod method, String payload) {
        logger.info(alertProxyConfiguration.toString());
        if (!alertProxyConfiguration.getEnabled()) {
            logger.info("alertProxyConfiguration.getEnabled()=" + alertProxyConfiguration.getEnabled());
            return Future.succeededFuture();
        }

        if (StringUtils.isNullOrEmpty(alertProxyConfiguration.getUrl())) {
            return Future.failedFuture("AlertProxyConfiguration URL is empty and must be populated");
        }

        final Future<HttpResponseContainer> future = Future.future();

        final HttpClientRequest httpClientRequest = httpClient.requestAbs(method, alertProxyConfiguration.getUrl())
                .putHeader("Authorization", HttpUtil.generateBasicAuthHeaderEntry(
                        alertProxyConfiguration.getUsername(), alertProxyConfiguration.getPassword())
                )
                .putHeader("pg-trx-id", UUID.randomUUID().toString())
                .putHeader("Content-Type", "application/json")
                .setTimeout((long) alertProxyConfiguration.getTimeoutSec() * 1000)
                .handler(response -> handleResponse(response, alertProxyConfiguration.getUrl(), future))
                .exceptionHandler(exception -> handleExceptionResponse(
                        exception, alertProxyConfiguration.getUrl(), future)
                );
        httpClientRequest.end(payload);

        return future;
    }

    private void handleResponse(HttpClientResponse response, String url, Future<HttpResponseContainer> future) {
        response
                .bodyHandler(buffer -> successResponse(buffer.toString(), response, future))
                .exceptionHandler(exception -> handleExceptionResponse(exception, url, future));
    }

    private void successResponse(
            String body, HttpClientResponse response, Future<HttpResponseContainer> future
    ) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            String msg = String.format("Sending alert received Non-200 HTTP code::%s. Response body::%s",
                    statusCode, body);
            logger.error(msg);
            future.tryFail(msg);
        } else {
            future.tryComplete(HttpResponseContainer.of(response.statusCode(), response.headers(), body));
        }
    }

    private void handleExceptionResponse(Throwable exception, String url, Future<HttpResponseContainer> future) {
        logger.error("Error occurred during sending alert to proxy at {0}::{1} ", url, exception.toString());
        future.tryFail(exception);
    }
}
