package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Configuration properties for refresh line item delivery plans from planner adapters.
 */

@Configuration
@Data
@ToString(exclude = {"password"})
@ConfigurationProperties(prefix = "services.planner-adapters")
public class PlannerAdapterConfigurations {

    @NotNull
    Integer dbStoreBatchSize;

    List<PlannerAdapterConfiguration> planners;

    @Data
    @ToString(exclude = {"password"})
    public static class PlannerAdapterConfiguration {

        @NotNull
        private String name;

        @NotNull
        private String bidderCodePrefix;

        @NotNull
        private Boolean enabled;

        @NotNull
        private String username;

        @NotNull
        private String password;

        @NotNull
        private String url;

        @NotNull
        private Integer initialDelaySec;

        @NotNull
        private Integer refreshPeriodSec;

        @NotNull
        private Integer timeoutSec;
    }

}
