package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * A model for administration command.
 */

@Getter
@Setter
@ToString
public class AdminCommand {

    private String cmd;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ObjectNode body;
}
