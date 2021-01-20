package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * The filters used by the tracer.
 */

@Getter
@Setter
@ToString
public class TracerFilters {

    String accountId;

    String bidderCode;

    String lineItemId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    String region;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    String vendor;

    void setNull() {
        accountId = null;
        bidderCode = null;
        lineItemId = null;
        region = null;
        vendor = null;
    }
}
