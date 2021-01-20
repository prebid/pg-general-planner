package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;

/**
 * A model to represent the allocated weight of a line item.
 */

@Data
@Builder
@ToString(exclude = {"uniqueLineItemId"})
@EqualsAndHashCode(exclude = {"uniqueLineItemId", "scaledWeight"})
public class Weightage {

    private Double weight;

    private String bidderCode;

    // this is the extLineItemId now
    private String lineItemId;

    // internal use only, lazy initialization, not store in DB.
    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private String uniqueLineItemId;

    // internal used for reallocation share calculation
    @JsonIgnore
    private long scaledWeight;

    public String getUniqueLineItemId() {
        if (uniqueLineItemId == null) {
            if (lineItemId == null) {
                throw new IllegalStateException("Weightage is not fully initialized.");
            }
            uniqueLineItemId = String.format("%s-%s", bidderCode, lineItemId);
        }
        return uniqueLineItemId;
    }
}


