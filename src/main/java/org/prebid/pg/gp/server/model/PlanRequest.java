package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * A model to represent the request for line item delivery plans from a PBS server.
 */

@Data
@Builder
@ToString
public class PlanRequest {

    private String vendor;

    private String region;

    private String instanceId;

}

