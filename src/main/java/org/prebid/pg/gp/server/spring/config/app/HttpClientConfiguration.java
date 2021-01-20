package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;

/**
 * Configuration properties for http client.
 */

@Configuration
@Data
@ToString
@ConfigurationProperties(prefix = "http-client")
public class HttpClientConfiguration {

    @NotNull
    private Integer maxPoolSize;

    @NotNull
    private Integer connectTimeoutSec;

    @NotNull
    private CircuitBreakerConfiguration circuitBreaker;

}
