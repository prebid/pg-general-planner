package org.prebid.pg.gp.server.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

/**
 * A object for server shutdown status.
 */

@Getter
@Setter
@ToString
@Component
public class Shutdown {

    private Boolean initiating = Boolean.FALSE;
}
