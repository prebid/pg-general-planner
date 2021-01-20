package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * A model to represent a pageable request.
 */

@Data
@Builder
@ToString
public class PageRequest {

    private int number;

    private int size;

}


