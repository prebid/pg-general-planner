package org.prebid.pg.gp.server.spring.config.app;

import lombok.Data;
import lombok.ToString;

import javax.validation.constraints.NotNull;

/**
 * Configuration for algorithm specification.
 */

@Data
@ToString
public class AlgorithmConfiguration {

    @NotNull
    private Integer nonAdjustableSharePercent;
}
