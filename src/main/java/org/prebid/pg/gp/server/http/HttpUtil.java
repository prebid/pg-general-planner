package org.prebid.pg.gp.server.http;

import java.util.Base64;

/**
 * A utility class for http.
 */
public class HttpUtil {
    private HttpUtil() {
    }

    /**
     * Encodes passed in {@code user} and {@code password} for http basic authentication request.
     *
     * @param user user name
     * @param password password
     *
     * @return encoded string
     */
    public static String generateBasicAuthHeaderEntry(String user, String password) {
        return String.format("Basic %s",
                Base64.getEncoder().encodeToString(String.format("%s:%s", user, password).getBytes()));
    }

}
