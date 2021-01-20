package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;

/**
 * Configuration properties for the job to do the token statistics of line items.
 */

@Configuration
@Data
@ToString
@ConfigurationProperties(prefix = "services.tokens-summary")
public class TokensSummaryConfiguration {

    @NotNull
    private Boolean enabled;

    @NotNull
    private Integer runOnMinute;

    @NotNull
    private Integer initialDelayMinute;

    @NotNull
    private Integer granularSummaryMinute;

}
