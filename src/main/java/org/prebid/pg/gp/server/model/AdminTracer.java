package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;

/**
 * Tracer for administration purpose.
 */

@Getter
@Setter
@ToString
public class AdminTracer {

    private String cmd;

    private Boolean raw = false;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean enabled = false;

    private Integer durationInSeconds;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Instant expiresAt = Instant.EPOCH;

    @Getter
    @Setter
    private TracerFilters filters = new TracerFilters();

    public synchronized void setTracer(AdminTracer tracerIn, int maxDurationInSeconds) {
        if (tracerIn == null || "stop".equalsIgnoreCase(tracerIn.cmd)) {
            enabled = false;
        } else {
            enabled = true;
        }

        if (enabled) {
            if (tracerIn.durationInSeconds > maxDurationInSeconds) {
                tracerIn.durationInSeconds = maxDurationInSeconds;
            }
            expiresAt = Instant.now().plus(Duration.ofSeconds(tracerIn.durationInSeconds));
            filters.setAccountId(tracerIn.filters.accountId);
            if (tracerIn.filters.bidderCode != null) {
                filters.bidderCode = tracerIn.filters.bidderCode.toUpperCase();
            }
            filters.lineItemId = tracerIn.filters.lineItemId;
            filters.region = tracerIn.filters.region;
            filters.vendor = tracerIn.filters.vendor;
            raw = tracerIn.raw;
            durationInSeconds = tracerIn.durationInSeconds;
        } else {
            expiresAt = Instant.EPOCH;
            durationInSeconds = 0;
            filters.setNull();
        }
    }

    private boolean checkIfActive() {
        return enabled && Instant.now().isBefore(expiresAt);
    }

    private boolean checkIfActiveAndRaw() {
        return enabled && Instant.now().isBefore(expiresAt) && raw;
    }

    public boolean checkActive() {
        boolean status = checkIfActive();
        if (!status) {
            setTracer(null, 0);
        }
        return status;
    }

    public boolean checkActiveAndRaw() {
        boolean status = checkIfActiveAndRaw();
        if (!status) {
            setTracer(null, 0);
        }
        return status;
    }

    public boolean matchAccount(String accountIdData) {
        return StringUtils.isEmpty(filters.accountId) || filters.accountId.equals(accountIdData);
    }

    public boolean matchBidderCode(String bidderCodeData) {
        return StringUtils.isEmpty(filters.bidderCode)
                || filters.bidderCode.equalsIgnoreCase(bidderCodeData)
                || (bidderCodeData != null && bidderCodeData.toUpperCase().contains(filters.bidderCode.toUpperCase()));
    }

    public boolean matchLineItemId(String lineItemIdData) {
        return StringUtils.isEmpty(filters.lineItemId)
                || filters.lineItemId.equalsIgnoreCase(lineItemIdData)
                || (lineItemIdData != null && lineItemIdData.toUpperCase().contains(filters.lineItemId.toUpperCase()));
    }

    public boolean matchRegion(String regionData) {
        return StringUtils.isEmpty(filters.region) || filters.region.equals(regionData);
    }

    public boolean matchVendor(String vendorData) {
        return StringUtils.isEmpty(filters.vendor) || filters.vendor.equals(vendorData);
    }

    public boolean match(
            String vendorData, String regionData, String bidderCodeData, String lineItemIdData, String accountIdData) {
        return matchVendor(vendorData)
                && matchRegion(regionData)
                && matchBidderCode(bidderCodeData)
                && matchLineItemId(lineItemIdData)
                && matchAccount(accountIdData);
    }
}
