package org.prebid.pg.gp.server.services;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.ToString;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary.SummaryData;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.ReallocationWeights;
import org.prebid.pg.gp.server.model.Weightage;
import org.prebid.pg.gp.server.spring.config.app.AlgorithmConfiguration;
import org.prebid.pg.gp.server.util.Validators;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A host-based token reallocation algorithm, which does the token reallocation based on the target matched
 * parameter reported by each PBS server at configured interval of time.
 */
public class TargetMatchedBasedTokenReallocation implements HostBasedTokenReallocation {

    private static final Logger logger = LoggerFactory.getLogger(TargetMatchedBasedTokenReallocation.class);

    private final AlgorithmConfiguration config;

    private static final long RATIO = 100000;

    private static final long ONE_HUNDRED = 100 * RATIO;

    private static final long ZERO = 0;

    public TargetMatchedBasedTokenReallocation(AlgorithmConfiguration config) {
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public List<ReallocatedPlan> calculate(
            List<DeliveryTokenSpendSummary> tokenSpendSummaries,
            List<ReallocatedPlan> previousPlans,
            List<LineItem> activeLineItems,
            List<PbsHost> activeHosts) {
        Objects.requireNonNull(tokenSpendSummaries);
        Objects.requireNonNull(previousPlans);
        Validators.checkArgument(activeLineItems, !CollectionUtils.isEmpty(activeLineItems),
                "No active line items exist for reallocation.");
        Validators.checkArgument(activeHosts, !CollectionUtils.isEmpty(activeHosts),
                "No active hosts exists for reallocation.");

        final Set<String> activeLineItemIds = activeLineItems.stream()
                .map(LineItem::getUniqueLineItemId)
                .collect(Collectors.toSet());
        final Set<String> activeHostIds = activeHosts.stream()
                .map(PbsHost::getUniqueInstanceId)
                .collect(Collectors.toSet());
        logger.debug("Input: tokenSpendSummaries:{0}, previousPlans:{1}, activeLineItems:{2}, activeHosts:{3}",
                tokenSpendSummaries, previousPlans, activeLineItemIds, activeHosts);

        // <lineItemId, <UniqueHostId, Stats>>
        final Map<String, Map<String, DeliveryTokenSpendSummary>> lineItemToHostStatsMap = tokenSpendSummaries
                .stream()
                // filter out stats for inactive lineItems & inactive hosts
                .filter(it -> activeLineItemIds.contains(it.getLineItemId())
                        && activeHostIds.contains(it.getUniqueInstanceId())
                        && (it.getSummaryData() != null && it.getSummaryData().getTargetMatched() != null))
                .collect(Collectors.groupingBy(DeliveryTokenSpendSummary::getLineItemId,
                        Collectors.toMap(DeliveryTokenSpendSummary::getUniqueInstanceId, it -> it)));

        final List<ReallocatedPlan> previousAllocations = previousPlans.stream()
                .filter(previousPlan -> activeHostIds.contains(previousPlan.getUniqueInstanceId())
                        && previousPlan.getReallocationWeights() != null
                        && !CollectionUtils.isEmpty(previousPlan.getReallocationWeights().getWeights()))
                .peek(this::populateScaledWeights)
                .collect(Collectors.toList());
        final PreviousPlanSummary prePlanData =
                PreviousPlanSummary.build(activeLineItemIds, previousAllocations, lineItemToHostStatsMap);
        logger.debug("PreviousPlanSummary::{0}", prePlanData);

        final Map<String, ReallocatedPlan> newHostToAllocationMap = new HashMap<>();
        // loop through each active lineItem
        for (LineItem li : activeLineItems) {
            logger.debug("lineItemId::{0}", li.getUniqueLineItemId());
            // for all sorts of reason no stats data available for this line item
            if (!lineItemToHostStatsMap.containsKey(li.getUniqueLineItemId())) {
                allocateWithoutStats(activeHosts, li, newHostToAllocationMap, prePlanData);
            } else {
                allocateWithStats(activeHosts, li, newHostToAllocationMap,
                        lineItemToHostStatsMap.get(li.getUniqueLineItemId()), prePlanData);
            }
        }
        final List<ReallocatedPlan> newPlans = newHostToAllocationMap.values().stream()
                .peek(this::populateWeights)
                .collect(Collectors.toList());
        logger.debug("calculated reallocation plans:{0}", newPlans);
        return newPlans;
    }

    private void allocateWithoutStats(
            List<PbsHost> activeHosts,
            LineItem li,
            Map<String, ReallocatedPlan> newHostToAllocationMap,
            PreviousPlanSummary prePlanData) {
        logger.debug("allocate without stats data.");
        String lineItemId = li.getUniqueLineItemId();
        Map<String, Weightage> hostWeightageMap = prePlanData.getHostToWeightageMap(lineItemId);
        final long average = averageShare(activeHosts.size());
        if (hostWeightageMap.isEmpty()) {
            logger.debug("no previous plan, no stats data. averageWeight::{0}", average);
            // no previous plan, no stats
            for (PbsHost host : activeHosts) {
                addWeightToAllocationPlan(host, li, newHostToAllocationMap, average, host.getUniqueInstanceId());
            }
            return;
        }
        // has previous plan, no stats
        allocateSharesForAllHosts(activeHosts, li, newHostToAllocationMap, prePlanData,
                prePlanData.getLineItemToTotalShareMap().get(lineItemId));
    }

    private void allocateWithStats(
            List<PbsHost> activeHosts,
            LineItem li,
            Map<String, ReallocatedPlan> newHostToAllocationMap,
            Map<String, DeliveryTokenSpendSummary> hostStatsMap,
            PreviousPlanSummary prePlanData) {
        logger.debug("allocate with stats data.");
        String uniqueLineItemId = li.getUniqueLineItemId();
        Map<String, Weightage> hostWeightageMap = prePlanData.getHostToWeightageMap(uniqueLineItemId);
        HostDeliveryStatsSummary statsSummary =
                HostDeliveryStatsSummary.build(hostStatsMap, prePlanData, uniqueLineItemId);
        logger.debug("allocateWithStats|lineItemId::{0}|statsSummary.getTotalMatched::{0}",
                li.getUniqueLineItemId(), statsSummary.getTotalMatched());
        long allocatedWeight = ZERO;
        double pc = 0.01 * config.getNonAdjustableSharePercent();
        long share = Math.round((1 - pc) * prePlanData.getLineItemToTotalShareMap().get(uniqueLineItemId));

        for (PbsHost host : activeHosts) {
            DeliveryTokenSpendSummary stats = statsSummary.getStatsInPlan().get(host.getUniqueInstanceId());
            if (stats != null) {
                int targetMatched = stats.getSummaryData().getTargetMatched();
                logger.debug("allocateWithStats::host={0}|targetMatched={1}|totalTargetMatched={2}",
                        host.getUniqueInstanceId(),
                        stats.getSummaryData() == null ? "" : targetMatched,
                        statsSummary.getTotalMatched()
                );
                Weightage weightage = hostWeightageMap.get(host.getUniqueInstanceId());
                long newWeight = weightage.getScaledWeight();
                if (statsSummary.getTotalMatched() > 0) {
                    newWeight = Math.round(pc * weightage.getScaledWeight()
                            + 1.0 * share * targetMatched / statsSummary.getTotalMatched());
                }
                allocatedWeight += newWeight;
                logger.debug("allocateWithStats:newWeight::{0}|allocatedWeight::{1}",
                        newWeight, allocatedWeight);
                // directly modify previous allocation weight
                weightage.setScaledWeight(newWeight);
            }
        }
        allocateSharesForAllHosts(activeHosts, li, newHostToAllocationMap, prePlanData, allocatedWeight);
    }

    private void allocateSharesForAllHosts(List<PbsHost> activeHosts,
            LineItem li,
            Map<String, ReallocatedPlan> newHostToAllocationMap,
            PreviousPlanSummary prePlan,
            long allocatedWeight) {
        final long average = averageShare(activeHosts.size());
        final int newHosts = activeHosts.size() - prePlan.getHostsInPreActivePlan(li.getUniqueLineItemId()).size();
        final long allocatableShares = ONE_HUNDRED - newHosts * average - allocatedWeight;
        double migrationRatio = allocatedWeight == 0 ? 0.0 : allocatableShares * 1.0 / allocatedWeight;
        if (logger.isDebugEnabled()) {
            logger.debug("allocateSharesForAllHosts:newHosts::{0}|averageShare::{1}|allocatableShares::{2}"
                            + "|migrationRatio::{3}",
                    newHosts, average, allocatableShares, migrationRatio);

        }
        Map<String, Weightage> hostWeightageMap = prePlan.getHostToWeightageMap(li.getUniqueLineItemId());
        for (PbsHost host : activeHosts) {
            long newShare = average;
            Weightage weightage = hostWeightageMap.get(host.getUniqueInstanceId());
            if (weightage != null && weightage.getScaledWeight() > 0) {
                newShare = Math.round(weightage.getScaledWeight() * (1.0 + migrationRatio));
            }
            logger.debug("allocateWithStats:newShare::{0}", newShare);
            addWeightToAllocationPlan(host, li, newHostToAllocationMap, newShare, host.getUniqueInstanceId());
        }
    }

    private void populateScaledWeights(ReallocatedPlan plan) {
        for (Weightage weightage : plan.getReallocationWeights().getWeights()) {
            weightage.setScaledWeight((long) (RATIO * weightage.getWeight()));
        }
    }

    private void populateWeights(ReallocatedPlan plan) {
        for (Weightage weightage : plan.getReallocationWeights().getWeights()) {
            weightage.setWeight(weightage.getScaledWeight() * 1.0 / RATIO);
        }
    }

    private static long averageShare(int count) {
        if (count == 0) {
            throw new IllegalArgumentException();
        }
        return Math.round(1.0 * ONE_HUNDRED / count);
    }

    private void addWeightToAllocationPlan(PbsHost host, LineItem lineItem,
            Map<String, ReallocatedPlan> newHostToAllocationMap, long share, String uniqueInstanceId) {
        final ReallocatedPlan allocation = newHostToAllocationMap.computeIfAbsent(uniqueInstanceId,
                key -> ReallocatedPlan.builder()
                        .vendor(host.getVendor())
                        .region(host.getRegion())
                        .instanceId(host.getHostInstanceId())
                        .reallocationWeights(ReallocationWeights.builder()
                                .weights(new ArrayList<>())
                                .build())
                        .build());
        allocation.addWeightage(Weightage.builder()
                .bidderCode(lineItem.getBidderCode())
                .lineItemId(lineItem.getLineItemId())
                .scaledWeight(share)
                .build());
    }

    @ToString
    static class PreviousPlanSummary {
        // <lineItemId, <UniqueHostId, Weightage>>
        private final Map<String, Map<String, Weightage>> lineItemToHostWeightageMap = new HashMap<>();

        // <lineItemId, total Share>
        private final Map<String, Long> lineItemToTotalShareMap = new HashMap<>();

        private final Map<String, Set<String>> hostsInPreActivePlanMap = new HashMap<>();

        private void buildSummary(Set<String> activeLineItemIds, List<ReallocatedPlan> previousAllocations,
                Map<String, Map<String, DeliveryTokenSpendSummary>> lineItemToHostStatsMap) {
            for (ReallocatedPlan plan : previousAllocations) {
                for (Weightage weightage : plan.getReallocationWeights().getWeights()) {
                    final String uniqueLineItemId = weightage.getUniqueLineItemId();
                    if (activeLineItemIds.contains(uniqueLineItemId)) {
                        // filter out inactive lineItems
                        lineItemToHostWeightageMap.computeIfAbsent(uniqueLineItemId, key -> new HashMap<>())
                                .put(plan.getUniqueInstanceId(), weightage);
                        final long totalShare = lineItemToTotalShareMap.getOrDefault(uniqueLineItemId, ZERO);
                        lineItemToTotalShareMap.put(uniqueLineItemId, totalShare + weightage.getScaledWeight());
                        hostsInPreActivePlanMap.computeIfAbsent(uniqueLineItemId, key -> new HashSet<>())
                                .add(plan.getUniqueInstanceId());
                    }
                }
            }
            // for new line items that do in memory shares allocation in PlanRequestHandler, fake reallocation plan per
            // stats data, so that share migration can happens in this round of reallocation
            for (Map.Entry<String, Map<String, DeliveryTokenSpendSummary>> entry : lineItemToHostStatsMap.entrySet()) {
                final String uniqueLineItemId = entry.getKey();
                long scaledWeight = averageShare(entry.getValue().size());
                if (!lineItemToTotalShareMap.containsKey(uniqueLineItemId)) {
                    for (Map.Entry<String, DeliveryTokenSpendSummary> hostStats : entry.getValue().entrySet()) {
                        DeliveryTokenSpendSummary stats = hostStats.getValue();
                        Weightage weightage = Weightage.builder()
                                .bidderCode(stats.getBidderCode())
                                .lineItemId(stats.getExtLineItemId())
                                .scaledWeight(scaledWeight)
                                .build();
                        lineItemToHostWeightageMap.computeIfAbsent(uniqueLineItemId, key -> new HashMap<>())
                                .put(hostStats.getKey(), weightage);
                        final long totalShare = lineItemToTotalShareMap.getOrDefault(uniqueLineItemId, ZERO);
                        lineItemToTotalShareMap.put(uniqueLineItemId, totalShare + weightage.getScaledWeight());
                        hostsInPreActivePlanMap.computeIfAbsent(uniqueLineItemId, key -> new HashSet<>())
                                .add(hostStats.getKey());
                    }
                }
            }
        }

        static PreviousPlanSummary build(Set<String> activeLineItemIds, List<ReallocatedPlan> previousAllocations,
                Map<String, Map<String, DeliveryTokenSpendSummary>> lineItemToHostStatsMap) {
            Objects.requireNonNull(activeLineItemIds);
            Objects.requireNonNull(previousAllocations);
            Objects.requireNonNull(lineItemToHostStatsMap);

            final PreviousPlanSummary summary = new PreviousPlanSummary();
            summary.buildSummary(activeLineItemIds, previousAllocations, lineItemToHostStatsMap);
            return summary;
        }

        Map<String, Weightage> getHostToWeightageMap(String lineItemId) {
            return lineItemToHostWeightageMap.computeIfAbsent(lineItemId, key -> new HashMap<>());
        }

        Map<String, Long> getLineItemToTotalShareMap() {
            return this.lineItemToTotalShareMap;
        }

        Set<String> getHostsInPreActivePlan(String uniqueLineItemId) {
            return hostsInPreActivePlanMap.get(uniqueLineItemId);
        }
    }

    static class HostDeliveryStatsSummary {
        private final Map<String, DeliveryTokenSpendSummary> statsInPlan = new HashMap<>();

        private int totalMatched;

        static HostDeliveryStatsSummary build(
                Map<String, DeliveryTokenSpendSummary> hostStatsMap,
                PreviousPlanSummary planSummary,
                String uniqueLineItemId) {
            Objects.requireNonNull(hostStatsMap);
            // spendSummaries must be sanitized first before passed in here
            final HostDeliveryStatsSummary summary = new HostDeliveryStatsSummary();
            summary.buildSummary(hostStatsMap, planSummary, uniqueLineItemId);
            return summary;
        }

        private void buildSummary(Map<String, DeliveryTokenSpendSummary> hostStatsMap,
                PreviousPlanSummary planSummary, String uniqueLineItemId) {
            Map<String, Weightage> planNoStatsMap = new HashMap<>();
            for (Map.Entry<String, Weightage> entry : planSummary.getHostToWeightageMap(uniqueLineItemId).entrySet()) {
                String uniqueInstanceId = entry.getKey();
                if (hostStatsMap.containsKey(uniqueInstanceId)) {
                    DeliveryTokenSpendSummary stats = hostStatsMap.get(uniqueInstanceId);
                    statsInPlan.put(uniqueInstanceId, stats);
                    totalMatched += stats.getSummaryData().getTargetMatched();
                } else {
                    planNoStatsMap.put(uniqueInstanceId, entry.getValue());
                }
            }
            int averageMatched = totalMatched / statsInPlan.size();
            for (Map.Entry<String, Weightage> entry : planNoStatsMap.entrySet()) {
                // if missing stats data, fake one with average targetMatched value, wait next run to
                // further adjustment if stats data available
                String[] arr = entry.getKey().split("\\^\\^");
                Weightage weight = entry.getValue();
                DeliveryTokenSpendSummary statsSummary = DeliveryTokenSpendSummary.builder()
                        .vendor(arr[0])
                        .region(arr[1])
                        .instanceId(arr[2])
                        .lineItemId(uniqueLineItemId)
                        .extLineItemId(weight.getLineItemId())
                        .bidderCode(weight.getBidderCode())
                        .summaryData(SummaryData.builder().targetMatched(averageMatched).build())
                        .build();
                statsInPlan.put(entry.getKey(), statsSummary);
                totalMatched += averageMatched;
            }
        }

        int getTotalMatched() {
            return this.totalMatched;
        }

        Map<String, DeliveryTokenSpendSummary> getStatsInPlan() {
            return this.statsInPlan;
        }
    }

}
