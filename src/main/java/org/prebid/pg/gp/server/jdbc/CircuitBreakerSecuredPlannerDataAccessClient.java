package org.prebid.pg.gp.server.jdbc;

import org.prebid.pg.gp.server.breaker.CircuitBreakerSecuredClient;
import org.prebid.pg.gp.server.breaker.PlannerCircuitBreaker;
import io.vertx.core.Future;
import io.vertx.ext.sql.UpdateResult;
import org.prebid.pg.gp.server.model.AdminEvent;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.LineItemIdentity;
import org.prebid.pg.gp.server.model.LineItemsTokensSummary;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.PlanRequest;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.model.Registration;
import org.prebid.pg.gp.server.model.SystemState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link PlannerDataAccessClient} guarded by the circuit breaker.
 */
public class CircuitBreakerSecuredPlannerDataAccessClient extends CircuitBreakerSecuredClient {

    private final PlannerDataAccessClient plannerDataAccessClient;

    public CircuitBreakerSecuredPlannerDataAccessClient(
            PlannerDataAccessClient plannerDataAccessClient,
            PlannerCircuitBreaker plannerCircuitBreaker) {
        super(plannerCircuitBreaker);
        this.plannerDataAccessClient = Objects.requireNonNull(plannerDataAccessClient);
    }

    /**
     * Updates the host-based token reallocation plans.
     *
     * @param reallocatedPlans a list of {@link ReallocatedPlan}s
     * @param batchSize size of batch update
     * @return a future to indicate if update successfully
     */
    public Future<Void> updateReallocatedPlans(List<ReallocatedPlan> reallocatedPlans, int batchSize) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.updateReallocatedPlans(reallocatedPlans, batchSize)
                        .setHandler(future));
    }

    /**
     * Gets the {@link ReallocatedPlan} for the given {@code pbsHost}.
     *
     * @param pbsHost the host to retrieve reallocation plan for
     * @return a future of {@link ReallocatedPlan}
     */
    public Future<ReallocatedPlan> getReallocatedPlan(PbsHost pbsHost) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getReallocatedPlan(
                            pbsHost.getHostInstanceId(), pbsHost.getRegion(), pbsHost.getVendor())
                        .setHandler(future));
    }

    /**
     * Gets the latest {@link ReallocatedPlan}s updated no earlier than the given {@code updatedSince}.
     *
     * @param updatedSince updated timestamp
     * @return a future of list of {@code ReallocatedPlan}s
     */
    public Future<List<ReallocatedPlan>> getLatestReallocatedPlans(Instant updatedSince) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getLatestReallocatedPlans(updatedSince)
                        .setHandler(future));
    }

    /**
     * Get the line items whose status is the same as given {@code status} and
     * end date is after the given {@code endTime}.
     *
     * @param status status of line item
     * @param endTime end date of line item
     * @return a future of list of line items
     */
    public Future<List<LineItem>> getLineItemsByStatus(String status, Instant endTime) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getLineItemsByStatus(status, endTime)
                        .setHandler(future));
    }

    /**
     * Get compact identity information({@code lineItemId} and {@code bidderCode})
     * of line items whose status is the same as given {@code status} and end
     * date is after the given {@code endTime}.
     *
     * @param status status of line item
     * @param endTime end date of line item
     * @return a future of list of line items only populated with identity information
     */
    public Future<List<LineItem>> getCompactLineItemsByStatus(String status, Instant endTime) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getCompactLineItemsByStatus(status, endTime)
                        .setHandler(future));
    }

    public Future<List<LineItem>> getLineItemsInactiveSince(Instant timestamp) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getLineItemsInactiveSince(timestamp)
                        .setHandler(future));
    }

    /**
     * Find the host which meet the given {@code planRequest} and has been actively
     * registered with General Planner since the given {@code activeSince} time.
     *
     * @param planRequest the {@link PlanRequest} instance
     * @param activeSince the time since when the host has been active registration with system
     * @return a future of {@code PbsHost}
     */
    public Future<PbsHost> findActiveHost(PlanRequest planRequest, Instant activeSince) {
        final PbsHost pbsHost = PbsHost.builder()
                .vendor(planRequest.getVendor())
                .hostInstanceId(planRequest.getInstanceId())
                .region(planRequest.getRegion())
                .build();
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.findActiveHost(pbsHost, activeSince)
                        .setHandler(future));
    }

    /**
     * Find the hosts which have been actively registered with General Planner
     * since the given {@code activeSince} time.
     *
     * @param activeSince the time since when the hosts has been active registration with system
     * @return a future of list of {@code PbsHost}s
     */
    public Future<List<PbsHost>> findActiveHosts(Instant activeSince) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.findActiveHosts(activeSince)
                        .setHandler(future));
    }

    /**
     * Retrieves line item token delivery statistics information meet the given criteria.
     *
     * @param hostInstanceId the id of the host
     * @param region the region of the host
     * @param vendor the vendor of the host
     * @param updatedSince the time that data updated after
     * @return a future of list of {@link DeliveryTokenSpendSummary}s.
     */
    public Future<List<DeliveryTokenSpendSummary>> readTokenSpendData(
            String hostInstanceId, String region, String vendor, Instant updatedSince) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.readTokenSpendData(hostInstanceId, region, vendor, updatedSince)
                        .setHandler(future));
    }

    /**
     * Gets the delivery token summary information of line items that meet the given criteria.
     *
     * @param startTime the earliest time for the start of summary time window
     * @param endTime the latest time for the end of summary time window
     * @param lineItemIds ids of line items
     * @return a future of list of {@link LineItemsTokensSummary}s
     */
    public Future<List<LineItemsTokensSummary>> getLineItemsTokensSummary(Instant startTime, Instant endTime,
            List<String> lineItemIds) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getLineItemsTokensSummary(startTime, endTime, lineItemIds)
                        .setHandler(future));
    }

    /**
     * Gets the delivery token hourly summary information for the line items that meet the given criteria.
     *
     * @param updatedAtOrAfter earliest update time(inclusive)
     * @param updatedBefore latest update time(exclusive)
     * @param lineItemIds ids of line items
     * @return future of a list of {@link LineItemsTokensSummary}s
     */
    public Future<List<LineItemsTokensSummary>> getHourlyLineItemTokens(
            Instant updatedAtOrAfter, Instant updatedBefore, List<LineItemIdentity> lineItemIds) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.getHourlyLineItemTokens(
                        updatedAtOrAfter, updatedBefore, lineItemIds)
                .setHandler(future));
    }

    /**
     * Updates line items with the given {@code lineItems} information.
     *
     * @param lineItems the information to be updated
     * @param batchSize the batch size for update operation
     * @return a future to indicate update result
     */
    public Future<Void> updateLineItems(List<LineItem> lineItems, int batchSize) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.updateLineItems(lineItems, batchSize)
                        .setHandler(future));
    }

    /**
     * Updates line item delivery statistics with the given {@code deliveryDataList}.
     *
     * @param deliveryDataList the delivery statistics to be updated
     * @param batchSize the batch size of update operation
     * @return a future to indicate update result
     */
    public Future<Void> updateDeliveryStats(List<DeliveryTokenSpendSummary> deliveryDataList, int batchSize) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.updateDeliveryData(deliveryDataList, batchSize)
                        .setHandler(future));
    }

    /**
     * Updates the Pbs Host registration information.
     *
     * @param registration the registration information
     * @return a future of {@link UpdateResult}
     */
    public Future<UpdateResult> updateRegistration(Registration registration) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.updateRegistration(registration)
                        .setHandler(future));
    }

    /**
     * Finds Pbs Host registrations that meet the given search criteria.
     *
     * @param activeSince earliest registration time
     * @param vendor the vendor of Pbs Host
     * @param region the region of Pbs Host
     * @param hostInstanceId the id of the Pbs Host
     * @return future of a list of map, whose value is latest registration timestamp
     */
    public Future<List<Map<String, Object>>> findRegistrations(
            Instant activeSince, String vendor, String region, String instanceId) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.findRegistrations(activeSince, vendor, region, instanceId)
                        .setHandler(future));
    }

    /**
     * Gets the system state information whose tag is the same as the given {@code tag}.
     *
     * @param tag the tag of the system state
     * @return a future of {@code} String that describes the system state
     */
    public Future<String> getSystemState(String tag) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.readUTCTimeValFromSystemState(tag)
                        .setHandler(future));
    }

    /**
     * Updates the system state with the given {@code systemState} information.
     *
     * @param systemState the system state to be updated
     * @return a future of {@code UpdatedResult}
     */
    public Future<UpdateResult> updateSystemStateWithUTCTime(SystemState systemState) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.updateSystemStateWithUTCTime(systemState)
                        .setHandler(future));
    }

    /**
     * Updates the administration events.
     *
     * @param entities the administration events to be updated
     * @param batchSize size of batch update
     *
     * @return a future to indicate update result
     */
    public Future<Void> updateAdminEvents(List<AdminEvent> entities, int batchSize) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.updateAdminEvents(entities, batchSize).setHandler(future));
    }

    /**
     * Finds earliest {@link AdminEvent} after given {@code expiryAt}.
     *
     * @param app name of the application
     * @param registration server registration information
     * @param expiryAt the expiration timestamp
     *
     * @return the future of {@code AdminEvent}
     */
    public Future<AdminEvent> findEarliestActiveAdminEvent(String app, Registration registration, Instant expiryAt) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.findEarliestActiveAdminEvent(app, registration, expiryAt)
                        .setHandler(future));
    }

    /**
     * Deletes the {@link AdminEvent} with the given {@code id}.
     *
     * @param id id of the {@code AdminEvent}
     *
     * @return a future of {@link UpdateResult}
     */
    public Future<UpdateResult> deleteAdminEvent(String id) {
        return plannerCircuitBreaker.executeCommand(
                future -> plannerDataAccessClient.deleteAdminEvent(id).setHandler(future));
    }

    /**
     * Gets the instance of {@link PlannerDataAccessClient} used in this object.
     *
     * @return the instance of {@code PlannerDataAccessClient}
     */
    public PlannerDataAccessClient getPlannerDataAccessClient() {
        return plannerDataAccessClient;
    }

}
