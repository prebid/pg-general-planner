package org.prebid.pg.gp.server.util;

/**
 * Utility class for handling string representation of time.
 */
public final class TimeUtil {

    private TimeUtil() {
    }

    public static String toTimeWithoutMiliSeconds(String time) {
        if (time == null || time.isEmpty()) {
            return time;
        }
        return time.replaceAll("\\.\\d*", "");
    }

}
