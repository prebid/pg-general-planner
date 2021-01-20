package org.prebid.pg.gp.server.spring.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.pg.gp.server.metric.Metrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for graphite metrics.
 */

@Configuration
class MetricsConfiguration {

    static final String METRIC_REGISTRY_NAME = "planner-metric-registry";

    @Bean
    Metrics metrics(MetricRegistry metricRegistry) {
        return new Metrics(metricRegistry);
    }

    @Bean
    MetricRegistry metricRegistry() {
        return SharedMetricRegistries.getOrCreate(METRIC_REGISTRY_NAME);
    }

    @Bean
    @ConditionalOnProperty(prefix = "metrics.graphite", name = "enabled", havingValue = "true")
    ScheduledReporter graphiteReporter(GraphiteProperties graphiteProperties, MetricRegistry metricRegistry) {
        final Graphite graphite = new Graphite(graphiteProperties.getHost(), graphiteProperties.getPort());
        final ScheduledReporter reporter = GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith(graphiteProperties.getPrefix())
                .build(graphite);
        reporter.start(graphiteProperties.getInterval(), TimeUnit.SECONDS);
        return reporter;
    }

    @Component
    @ConfigurationProperties(prefix = "metrics.graphite")
    @ConditionalOnProperty(prefix = "metrics.graphite", name = "enabled", havingValue = "true")
    @Validated
    @Data
    @NoArgsConstructor
    private static class GraphiteProperties {
        @NotBlank
        private String prefix;

        @NotBlank
        private String host;

        @NotNull
        private Integer port;

        @NotNull
        @Min(1)
        private Integer interval;
    }

}
