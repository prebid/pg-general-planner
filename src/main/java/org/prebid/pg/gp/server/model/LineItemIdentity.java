package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Data;

/**
 * The id of a {@link LineItem}.
 */

@Builder
@Data
public class LineItemIdentity {

    private String lineItemId;

    private String bidderCode;

}

