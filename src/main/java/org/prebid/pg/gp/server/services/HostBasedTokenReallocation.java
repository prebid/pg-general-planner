package org.prebid.pg.gp.server.services;

import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.ReallocatedPlan;

import java.util.List;

/**
 * An algorithm to do the host-based token reallocation.
 */
@FunctionalInterface
public interface HostBasedTokenReallocation {

    /**
     * Does host-based token reallocation.
     *
     * @param tokenSpendSummaries a list of {@link DeliveryTokenSpendSummary}s
     * @param previousPlans existing {@link ReallocatedPlan}s
     * @param activeLineItems currently active line items
     * @param activeHosts currently active PBS servers
     *
     * @return host-based token reallocation result
     */
    List<ReallocatedPlan> calculate(
            List<DeliveryTokenSpendSummary> tokenSpendSummaries,
            List<ReallocatedPlan> previousPlans,
            List<LineItem> activeLineItems,
            List<PbsHost> activeHosts);

}


