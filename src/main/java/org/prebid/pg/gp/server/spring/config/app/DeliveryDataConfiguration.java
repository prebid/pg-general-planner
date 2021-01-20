package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Configuration properties for refresh line item delivery stats data.
 */

@Configuration
@Data
@ToString(exclude = {"password"})
@ConfigurationProperties(prefix = "services.delivery-data")
public class DeliveryDataConfiguration {

    @NotNull
    private Boolean enabled;

    @NotNull
    private Integer initialDelaySec;

    @NotNull
    private Integer refreshPeriodSec;

    @NotNull
    private Integer startTimeInPastSec;

    @NotBlank
    private String url;

    @NotNull
    private String username;

    @NotNull
    private String password;

    @NotNull
    private Integer timeoutSec;
}
