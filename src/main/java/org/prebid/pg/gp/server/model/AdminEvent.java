package org.prebid.pg.gp.server.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Instant;

/**
 * A model for administration event.
 */

@Data
@Builder
@ToString
public class AdminEvent {

    private String id;

    private String app;

    private String vendor;

    private String region;

    private String instanceId;

    private Instant expiryAt;

    private Instant createdAt;

    private Directive directive;

    @Builder
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Directive {

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

}
