package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * A wrapper object to represent a list of {@link Weightage}s.
 */

@Data
@Builder
@ToString
public class ReallocationWeights {

    private List<Weightage> weights;

}
