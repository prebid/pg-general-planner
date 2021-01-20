package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;

/**
 * Configuration properties for some data access behaviors.
 */

@Configuration
@Data
@ToString
@ConfigurationProperties(prefix = "api.line-items-tokens-summary")
public class LineItemsTokensSummaryConfiguration {

    @NotNull
    private Integer pageSize;

}
