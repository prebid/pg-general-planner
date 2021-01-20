package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * The source of an alert.
 */

@Getter
@Setter
@ToString
@Builder
public class AlertSource {

    private String env;

    @JsonProperty("data-center")
    private String dataCenter;

    private String region;

    private String system;

    @JsonProperty("sub-system")
    private String subSystem;

    @JsonProperty("host-id")
    private String hostId;
}
