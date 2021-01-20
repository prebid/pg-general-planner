package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

/**
 * Configuration properties for circuit breaker.
 */

@Data
@Validated
@ToString
public class CircuitBreakerConfiguration {

    @NotNull
    private Integer openingThreshold;

    @NotNull
    private Integer closingIntervalSec;

}
