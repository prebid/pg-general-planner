package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

/**
 * Configuration properties for the system deployment.
 */

@Configuration
@Data
@ToString
@Validated
@ConfigurationProperties(prefix = "deployment")
public class DeploymentConfiguration {

    @NotNull
    private String profile;

    @NotNull
    private String infra;

    @NotNull
    private String dataCenter;

    @NotNull
    private String region;

    @NotNull
    private String system;

    @NotNull
    private String subSystem;
}

