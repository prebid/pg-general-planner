package org.prebid.pg.gp.server.http;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.prebid.pg.gp.server.model.AlertEvent;
import org.prebid.pg.gp.server.model.AlertPriority;
import org.prebid.pg.gp.server.spring.config.app.AlertProxyConfiguration;
import org.prebid.pg.gp.server.spring.config.app.AlertProxyConfiguration.AlertPolicy;
import org.prebid.pg.gp.server.spring.config.app.CircuitBreakerConfiguration;
import org.prebid.pg.gp.server.spring.config.app.DeploymentConfiguration;
import org.prebid.pg.gp.server.spring.config.app.HttpClientConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.givenThat;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class AlertProxyHttpClientTest {

    private static Vertx vertx;

    private static WireMockServer wireMockServer;

    private AlertProxyHttpClient alertHttpClient;

    private AlertProxyConfiguration alertProxyConfig;

    @BeforeAll
    static void prepare() throws Exception {
        vertx = Vertx.vertx();
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
        configureFor(wireMockServer.port());
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        wireMockServer.shutdown();
        vertx.close();
    }

    @BeforeEach
    void setUpBeforeEach() {
        CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();
        circuitBreakerConfiguration.setClosingIntervalSec(30);
        circuitBreakerConfiguration.setOpeningThreshold(10);
        HttpClientConfiguration httpClientConfig = new HttpClientConfiguration();
        httpClientConfig.setConnectTimeoutSec(30);
        httpClientConfig.setMaxPoolSize(5);
        httpClientConfig.setCircuitBreaker(circuitBreakerConfiguration);
        DeploymentConfiguration deploymentConfig = new DeploymentConfiguration();
        alertProxyConfig = new AlertProxyConfiguration();
        alertProxyConfig.setEnabled(true);
        alertProxyConfig.setUsername("un");
        alertProxyConfig.setPassword("pwd");
        alertProxyConfig.setTimeoutSec(3);
        List<AlertPolicy> alertPolicies = new ArrayList<>();
        AlertPolicy alertPolicy = new AlertPolicy();
        alertPolicy.setAlertName("default");
        alertPolicy.setInitialAlerts(3);
        alertPolicy.setAlertFrequency(5);
        alertPolicies.add(alertPolicy);
        alertProxyConfig.setPolicies(alertPolicies);

        alertHttpClient = new AlertProxyHttpClient(
                vertx, httpClientConfig, deploymentConfig, alertProxyConfig, new ConcurrentHashMap<>());
    }

    @Test
    void shouldRaiseEvent() {
        givenThat(post(urlEqualTo("/notify"))
                .willReturn(aResponse().withStatus(200)));
        alertProxyConfig.setUrl("http://localhost:" + wireMockServer.port() + "/notify");
        Future<Void> result = alertHttpClient.raiseEvent("alert", AlertPriority.HIGH, "testing");
        assertThat(result.succeeded(), equalTo(true));
    }

    @Test
    void shouldFireAlert() {
        Set<Integer> trueSet = Arrays.asList(0, 1, 2, 7, 12, 17).stream().collect(Collectors.toSet());
        for (int i = 0; i < 18; i++) {
            AlertEvent event = AlertEvent.builder()
                    .name("testing-alert")
                    .priority(AlertPriority.MEDIUM)
                    .details("message" + i)
                    .build();
            boolean result = alertHttpClient.shouldFireAlert(event);
            assertThat(result, equalTo(trueSet.contains(i)));
        }
    }

    @Test
    void shouldFireAlertForDifferentPriority() {
        String alertName = "testing-alert";
        for (int i = 0; i < 3; i++) {
            AlertEvent event = AlertEvent.builder()
                    .name(alertName)
                    .priority(AlertPriority.MEDIUM)
                    .details("message" + i)
                    .build();
            boolean result = alertHttpClient.shouldFireAlert(event);
            assertThat(result, equalTo(true));
        }
        AlertEvent event3 = AlertEvent.builder()
                .name(alertName)
                .priority(AlertPriority.MEDIUM)
                .details("message")
                .build();
        boolean result = alertHttpClient.shouldFireAlert(event3);
        assertThat(result, equalTo(false));

        AlertEvent event4 = AlertEvent.builder()
                .name(alertName)
                .priority(AlertPriority.HIGH)
                .details("message")
                .build();
        result = alertHttpClient.shouldFireAlert(event4);
        assertThat(result, equalTo(true));
    }

    @Test
    void shouldClearAlert() {
        String eventName = "testing-alert";
        for (int i = 0; i < 3; i++) {
            AlertEvent event = AlertEvent.builder()
                    .name(eventName)
                    .priority(AlertPriority.MEDIUM)
                    .details("message" + i)
                    .build();
            boolean result = alertHttpClient.shouldFireAlert(event);
            assertThat(result, equalTo(true));
        }
        boolean rs = alertHttpClient.shouldFireAlert(
                AlertEvent.builder().name(eventName).priority(AlertPriority.MEDIUM).build());
        assertThat(rs, equalTo(false));
        // clear a different name alert
        alertHttpClient.clearAlert(AlertEvent.builder().name("foo").priority(AlertPriority.MEDIUM).build());
        rs = alertHttpClient.shouldFireAlert(
                AlertEvent.builder().name(eventName).priority(AlertPriority.MEDIUM).build());
        assertThat(rs, equalTo(false));

        alertHttpClient.clearAlert(AlertEvent.builder().name(eventName).priority(AlertPriority.MEDIUM).build());
        rs = alertHttpClient.shouldFireAlert(
                AlertEvent.builder().name(eventName).priority(AlertPriority.MEDIUM).build());
        assertThat(rs, equalTo(true));
    }
}
