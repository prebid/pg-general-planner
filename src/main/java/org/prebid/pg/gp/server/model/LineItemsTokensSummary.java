package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.time.Instant;

/**
 * An entity to represent statistics data for line item's tokens.
 */

@Data
@Builder
@ToString
public class LineItemsTokensSummary {

    private Integer id;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant summaryWindowStartTimestamp;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant summaryWindowEndTimestamp;

    private String lineItemId;

    private String bidderCode;

    private String extLineItemId;

    private int tokens;

    @JsonIgnore
    private Instant createdAt;

}
