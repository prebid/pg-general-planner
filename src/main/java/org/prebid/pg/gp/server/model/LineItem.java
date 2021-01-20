package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;
import lombok.ToString;
import org.prebid.pg.gp.server.util.Constants;
import org.prebid.pg.gp.server.util.JsonUtil;
import org.prebid.pg.gp.server.util.TimeUtil;
import org.springframework.util.StringUtils;

import java.time.Instant;
import javax.validation.constraints.NotBlank;

/**
 * An entity to represent the line item used in the system.
 */

@Data
@Builder
@ToString(exclude = {"uniqueLineItemId"})
public class LineItem {

    @NotBlank(message = "lineItemId is required.")
    private String lineItemId;

    // internal use, store in DB.
    @JsonIgnore
    @NotBlank(message = "bidderCode is required.")
    private String bidderCode;

    @NotBlank(message = "status is required.")
    private String status;

    private String startTimeStamp;

    @NotBlank(message = "endTimeStamp is required.")
    private String endTimeStamp;

    // internal use only, lazy initialization, not store in DB.
    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private String uniqueLineItemId;

    // lineItem in Json Object
    @JsonIgnore
    private ObjectNode lineItemJson;

    @JsonIgnore
    private Instant updatedAt;

    public String getUniqueLineItemId() {
        if (uniqueLineItemId == null) {
            if (lineItemId == null) {
                throw new IllegalStateException("Weightage is not fully initialized.");
            }
            uniqueLineItemId = String.format("%s-%s", bidderCode, lineItemId);
        }
        return uniqueLineItemId;
    }

    public static LineItem from(ObjectNode root, String adapterName, String bidderCodePrefix) {
        final String bidderCode = bidderCodePrefix + adapterName;
        final String startTimeStamp = TimeUtil.toTimeWithoutMiliSeconds(
                JsonUtil.optString(root, Constants.FIELD_START_TIME));
        final String endTimeStamp = TimeUtil.toTimeWithoutMiliSeconds(
                JsonUtil.optString(root, Constants.FIELD_END_TIME));
        final String updatedTimeStamp = TimeUtil.toTimeWithoutMiliSeconds(
                JsonUtil.optString(root, Constants.FIELD_UPDATED_TIME_STAMP));

        String status = JsonUtil.optString(root, Constants.FIELD_STATUS);
        status = status == null ? null : status.toLowerCase();

        final LineItem li = LineItem.builder()
                .bidderCode(bidderCode)
                .lineItemId(JsonUtil.optString(root, Constants.FIELD_LINE_ITEM_ID))
                .status(status)
                .startTimeStamp(startTimeStamp)
                .endTimeStamp(endTimeStamp)
                .lineItemJson(root)
                .build();

        JsonUtil.setValue(root, Constants.FIELD_START_TIME, startTimeStamp);
        JsonUtil.setValue(root, Constants.FIELD_END_TIME, endTimeStamp);
        JsonUtil.setValue(root, Constants.FIELD_UPDATED_TIME_STAMP, updatedTimeStamp);
        JsonUtil.updateArrayItemField(root, Constants.FIELD_FREQUENCY_CAPS, Constants.FIELD_FREQUENCY_CAP_ID,
                fcapId -> uniqueFreqCapId(fcapId, adapterName));
        JsonUtil.updateArrayItemField(root, Constants.FIELD_DELIVERY_SCHEDULES, Constants.FIELD_START_TIME,
                TimeUtil::toTimeWithoutMiliSeconds);
        JsonUtil.updateArrayItemField(root, Constants.FIELD_DELIVERY_SCHEDULES, Constants.FIELD_END_TIME,
                TimeUtil::toTimeWithoutMiliSeconds);
        return li;
    }

    private static String uniqueFreqCapId(String fcapId, String adapterName) {
        return StringUtils.isEmpty(fcapId) ? null : String.format("pg%s-%s", adapterName, fcapId);
    }

}

