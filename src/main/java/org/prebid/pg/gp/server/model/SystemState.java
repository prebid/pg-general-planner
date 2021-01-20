package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * An entity to represent system states.
 */

@Data
@Builder
@ToString
public class SystemState {

    private String tag;

    private String val;

}

