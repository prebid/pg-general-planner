package org.prebid.pg.gp.server.util;

import java.util.Objects;

/**
 * Utility class for handling String.
 */
public final class StringUtil {

    private StringUtil() {
    }

    public static StringBuilder appendRepeatedly(StringBuilder sb, String pattern, String delimiter, int times) {
        Objects.requireNonNull(sb);
        Objects.requireNonNull(delimiter);
        Validators.checkArgument(times, times > 0, "times should be larger than 0");
        int end = times - 1;
        for (int i = 0; i <= end; i++) {
            sb.append(pattern);
            if (i < end) {
                sb.append(delimiter);
            }
        }
        return sb;
    }
}
