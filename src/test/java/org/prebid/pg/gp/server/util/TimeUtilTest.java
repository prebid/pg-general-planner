package org.prebid.pg.gp.server.util;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class TimeUtilTest {

    @Test
    void shouldToTimeWithoutMiliSecondsHandleNull() {
        String rs = TimeUtil.toTimeWithoutMiliSeconds(null);
        assertThat(rs, is(nullValue()));
    }

    @Test
    void shouldToTimeWithoutMiliSecondsHandleEmptyString() {
        String rs = TimeUtil.toTimeWithoutMiliSeconds("");
        assertThat(rs.length(), equalTo(0));
    }
}
