package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * A model to represent token delivery statistics of a line item.
 */

@Data
@Builder
@ToString
public class Token {

    @JsonProperty("class")
    private Integer clazz;

    private Integer total;

}

