package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

/**
 * The payload of an alert.
 */

@Getter
@Setter
@ToString
@Builder
public class AlertPayload {

    private List<AlertEvent> events;
}
