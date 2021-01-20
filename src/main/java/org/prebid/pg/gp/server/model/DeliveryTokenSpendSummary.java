package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;
import org.prebid.pg.gp.server.util.TimeUtil;

import javax.validation.constraints.NotBlank;
import java.time.Instant;
import java.util.List;

/**
 * An entity to represent delivery statistics information of a line item.
 */

@Data
@Builder
@ToString(exclude = {"uniqueInstanceId"})
@EqualsAndHashCode(of = {"vendor", "region", "instanceId", "lineItemId"})
public class DeliveryTokenSpendSummary {

    private String reportId;

    @NotBlank(message = "vendor is required.")
    private String vendor;

    @NotBlank(message = "region is required.")
    private String region;

    @NotBlank(message = "instanceId is required.")
    private String instanceId;

    // for internal use, not store in DB, calculated from lineItemId and extLineItemId
    private String bidderCode;

    // in format of bidderCode-extLineItemId
    @NotBlank(message = "lineItemId is required.")
    private String lineItemId;

    // this is the original lineItemId
    @NotBlank(message = "extLineItemId is required.")
    private String extLineItemId;

    @NotBlank(message = "dataWindowStartTimestamp is required.")
    private String dataWindowStartTimestamp;

    @NotBlank(message = "dataWindowEndTimestamp")
    private String dataWindowEndTimestamp;

    // data comes in from external system
    private String reportTimestamp;

    // when retrieved from DB, use this field for time-based operation
    private Instant reportTime;

    private String serviceInstanceId;

    private SummaryData summaryData;

    private Instant updatedAt;

    // internal use only, lazy initialization, not store in DB
    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private String uniqueInstanceId;

    public void setDataWindowStartTimestamp(String val) {
        this.dataWindowStartTimestamp = TimeUtil.toTimeWithoutMiliSeconds(val);
    }

    public void setDataWindowEndTimestamp(String val) {
        this.dataWindowEndTimestamp = TimeUtil.toTimeWithoutMiliSeconds(val);
    }

    public void setReportTimeStamp(String val) {
        this.reportTimestamp = TimeUtil.toTimeWithoutMiliSeconds(val);
    }

    public String getUniqueInstanceId() {
        if (uniqueInstanceId == null) {
            if (instanceId == null) {
                throw new IllegalStateException("DeliveryTokenSpendSummary is not fully initialized.");
            }
            uniqueInstanceId = String.format("%s^^%s^^%s", vendor, region, instanceId);
        }
        return uniqueInstanceId;
    }

    @Data
    @Builder
    @ToString
    public static class TokenSpend {
        private Integer pc;

        @JsonProperty("class")
        private Integer clazz;
    }

    @Data
    @Builder
    @ToString
    public static class SummaryData {
        private List<TokenSpend> tokenSpent;

        private Integer targetMatched;
    }

}

