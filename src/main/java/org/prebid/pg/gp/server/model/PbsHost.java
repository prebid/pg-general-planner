package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

/**
 * An entity to represent a PBS host in the system.
 */

@Getter
@Builder
@ToString(exclude = {"uniqueInstanceId", "adReqsPerSec", "healthIndex", "createdAt"})
public class PbsHost {

    private String hostInstanceId;

    private String region;

    private String vendor;

    private Float healthIndex;

    private Integer adReqsPerSec;

    private Instant createdAt;

    // internal use only, lazy initialization, not store in DB
    @JsonIgnore
    @Setter(AccessLevel.PRIVATE)
    private String uniqueInstanceId;

    public String getUniqueInstanceId() {
        if (uniqueInstanceId == null) {
            if (hostInstanceId == null) {
                throw new IllegalStateException("PbsHost is not fully initialized.");
            }
            uniqueInstanceId = String.format("%s^^%s^^%s", vendor, region, hostInstanceId);
        }
        return uniqueInstanceId;
    }

}
