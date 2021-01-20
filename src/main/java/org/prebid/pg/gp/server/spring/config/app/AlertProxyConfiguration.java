package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Configuration properties for alerts.
 */

@Configuration
@Data
@ToString(exclude = {"password"})
@ConfigurationProperties(prefix = "services.alert-proxy")
public class AlertProxyConfiguration {

    @NotNull
    private Boolean enabled;

    @NotBlank
    private String url;

    @NotNull
    private Integer timeoutSec;

    @NotNull
    private String username;

    @NotNull
    private String password;

    private List<AlertPolicy> policies;

    @ToString
    @Data
    public static class AlertPolicy {

        @NotNull
        private String alertName;

        @NotNull
        private Integer initialAlerts;

        @NotNull
        private Integer alertFrequency;
    }

}
