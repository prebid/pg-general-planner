package org.prebid.pg.gp.server.util;

import javax.validation.ConstraintViolation;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for method argument checking and extract of error messages.
 */
public final class Validators {

    private Validators() {
    }

    public static <T> T checkArgument(T value, boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static int checkArgument(int value, boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static <T> String extractErrorMessages(Set<ConstraintViolation<T>> violations) {
        if (violations == null) {
            return "";
        }
        return violations.stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.joining(" ", "[", "]"));
    }
}

