package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;

/**
 * Configuration properties for doing host-based token reallocation.
 */

@Configuration
@Data
@ToString
@ConfigurationProperties(prefix = "services.host-reallocation")
public class HostReallocationConfiguration {

    @NotNull
    private Boolean enabled;

    @NotNull
    private Integer initialDelaySec;

    @NotNull
    private Integer refreshPeriodSec;

    @NotNull
    private Integer lineItemHasExpiredMin;

    @NotNull
    private Integer dbStoreBatchSize;

    @NotNull
    private Integer reallocationUpdatedSinceMin;

    @NotNull
    private String algorithm;

    @NotNull
    private AlgorithmConfiguration algorithmSpec;

}

