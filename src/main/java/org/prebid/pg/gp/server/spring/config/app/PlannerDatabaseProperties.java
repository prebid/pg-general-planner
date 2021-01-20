package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * Configuration properties for database access in General Planner.
 */

@Component
@Validated
@Data
@ToString(exclude = {"password"})
@ConfigurationProperties(prefix = "database.general-planner")
public class PlannerDatabaseProperties {

    @NotBlank
    String host;

    @NotNull
    Integer port;

    @NotBlank
    String dbname;

    @NotBlank
    String user;

    @NotBlank
    String password;

    @NotNull
    Integer initialPoolSize;

    @NotNull
    Integer minPoolSize;

    @NotNull
    Integer maxPoolSize;

    @NotNull
    Integer maxIdleTimeSec;

    @NotNull
    CircuitBreakerConfiguration circuitBreaker;

}
