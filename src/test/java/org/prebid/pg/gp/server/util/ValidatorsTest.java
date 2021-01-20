package org.prebid.pg.gp.server.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValidatorsTest {

    @Test
    void shouldCheckArgumentThrowsException() {
        Instant time = null;
        assertThrows(IllegalArgumentException.class, () -> Validators.checkArgument(time, time != null, "error"));
    }

    @Test
    void shouldCheckIntArgumentThrowsException() {
        int val = -1;
        assertThrows(IllegalArgumentException.class, () -> Validators.checkArgument(val, val > 0, "error"));
    }

    @Test
    void shouldCheckIntArgument() {
        int val = 1;
        int rs = Validators.checkArgument(val, val > 0, "error");
        assertThat(rs, equalTo(1));
    }
}
