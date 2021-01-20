package org.prebid.pg.gp.server.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

/**
 * A model to describe alerts.
 */

@Getter
@Setter
@ToString
@Builder
@EqualsAndHashCode(of = {"name", "priority"})
public class AlertEvent {

    private String id;

    private String action;

    private AlertPriority priority;

    private Instant updatedAt;

    private String name;

    private String details;

    private AlertSource source;
}
