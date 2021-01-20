package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.List;

/**
 * The payload of administration command.
 */

@Getter
@Setter
@ToString
public class AdminPayload {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String app;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String vendor;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> regions;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer expiresInMinutes;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Instant expiresAt;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private AdminTracer tracer;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private AdminCommand services;

    @JsonProperty("storedrequest")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private AdminCommand storedRequest;

    @JsonProperty("storedrequest-amp")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private AdminCommand storedRequestAmp;

}
