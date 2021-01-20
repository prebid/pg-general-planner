package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * Delivery schedule of a line item.
 */

@Data
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeliverySchedule {

    private List<Plan> deliverySchedules;

    @Data
    @ToString
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Plan {
        @JsonProperty("startTimeStamp")
        private String startTimestamp;

        @JsonProperty("endTimeStamp")
        private String endTimestamp;

        private List<Token> tokens;
    }

    @Data
    @ToString
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Token {

        private int total;

        @JsonProperty("class")
        private int clazz;
    }
}
