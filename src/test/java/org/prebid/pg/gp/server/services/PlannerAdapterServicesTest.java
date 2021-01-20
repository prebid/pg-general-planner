package org.prebid.pg.gp.server.services;

import com.codahale.metrics.MetricRegistry;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;
import org.prebid.pg.gp.server.http.AlertProxyHttpClient;
import org.prebid.pg.gp.server.http.CircuitBreakerSecuredPlannerAdapterHttpClient;
import org.prebid.pg.gp.server.jdbc.CircuitBreakerSecuredPlannerDataAccessClient;
import org.prebid.pg.gp.server.metric.Metrics;
import org.prebid.pg.gp.server.model.AdminTracer;
import org.prebid.pg.gp.server.model.Shutdown;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations;
import org.prebid.pg.gp.server.spring.config.app.PlannerAdapterConfigurations.PlannerAdapterConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class PlannerAdapterServicesTest {

    @Test
    public void shouldInitalizeSuccessfully() {
        PlannerAdapterConfigurations configs = new PlannerAdapterConfigurations();
        configs.setDbStoreBatchSize(1);
        List<PlannerAdapterConfiguration> configList = new ArrayList<>();
        PlannerAdapterConfiguration config = new PlannerAdapterConfiguration();
        config.setEnabled(false);
        config.setName("bar");
        
        configList.add(config);
        configs.setPlanners(configList);
        
        CircuitBreakerSecuredPlannerAdapterHttpClient httpClientMock =
                mock(CircuitBreakerSecuredPlannerAdapterHttpClient.class);
        CircuitBreakerSecuredPlannerDataAccessClient dataAccessClientMock =
                mock(CircuitBreakerSecuredPlannerDataAccessClient.class);
        final Map<String, CircuitBreakerSecuredPlannerAdapterHttpClient> httpClients = new HashMap<>();
        httpClients.put("bar", httpClientMock);

        AlertProxyHttpClient alertProxyHttpClientMock = mock(AlertProxyHttpClient.class);
        PlannerAdapterServices services = new PlannerAdapterServices(Vertx.vertx());
        services.initialize(
                "foo", configs, dataAccessClientMock, httpClients,
                new Metrics(new MetricRegistry()), new AdminTracer(), new Shutdown(), alertProxyHttpClientMock
        );
    }
}
