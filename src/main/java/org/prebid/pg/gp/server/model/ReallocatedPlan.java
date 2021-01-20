package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;

/**
 * An entity to represent line items token reallocation plan for a PBS host.
 */

@Data
@Builder
@ToString(exclude = {"uniqueInstanceId"})
public class ReallocatedPlan {

    private String serviceInstanceId;

    private String vendor;

    private String region;

    private String instanceId;

    private ReallocationWeights reallocationWeights;

    // internal use only, lazy initialization, not store in DB
    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private String uniqueInstanceId;

    private Instant updatedAt;

    @JsonIgnore
    public boolean isEmpty() {
        return vendor == null || region == null || instanceId == null;
    }

    public String getUniqueInstanceId() {
        if (uniqueInstanceId == null) {
            if (instanceId == null) {
                throw new IllegalStateException("ReallocatedPlan is not fully initialized.");
            }
            uniqueInstanceId = String.format("%s^^%s^^%s", vendor, region, instanceId);
        }
        return uniqueInstanceId;
    }

    public void addWeightage(Weightage weightage) {
        Objects.requireNonNull(weightage);

        if (reallocationWeights == null) {
            reallocationWeights = ReallocationWeights.builder()
                    .weights(new ArrayList<>())
                    .build();
        }
        reallocationWeights.getWeights().add(weightage);
    }

}
