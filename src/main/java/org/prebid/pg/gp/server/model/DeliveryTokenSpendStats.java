package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * Wrapper class for a list of {@link DeliveryTokenSpendSummary}.
 */

@Data
@ToString
@Builder
public class DeliveryTokenSpendStats {

    private List<DeliveryTokenSpendSummary> tokenSpendSummaryLines;

}
