package org.prebid.pg.gp.server.model;

import io.vertx.core.MultiMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

/**
 * A wrapper class for http response.
 */

@AllArgsConstructor(staticName = "of")
@Value
@Getter
@Builder
public class HttpResponseContainer {

    private int statusCode;

    private MultiMap headers;

    private String body;

}
