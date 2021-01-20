package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mysql.cj.core.util.StringUtils;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * A object to represent PBS host registration request.
 */

@Data
@Builder
@Accessors(chain = true)
@ToString(exclude = {"registrationId", "healthIndex", "adReqsPerSec", "createdAt"})
public class Registration {

    @JsonProperty("hostInstanceId")
    private String instanceId;

    private String vendor;

    private String region;

    private Float healthIndex;

    private Integer adReqsPerSec;

    private ObjectNode status;

    public List<String> validate() {
        List<String> missingFields = new ArrayList<>();

        validateRequiredFields("region", region, missingFields);
        validateRequiredFields("vendor", vendor, missingFields);
        validateRequiredFields("hostInstanceId", instanceId, missingFields);

        if (healthIndex == null || healthIndex < 0 || healthIndex > 1) {
            missingFields.add("healthIndex");
        }

        return missingFields;
    }

    private void validateRequiredFields(String field, String val, List<String> missingFields) {
        if (StringUtils.isNullOrEmpty(val)) {
            missingFields.add(field);
        }
    }

}

