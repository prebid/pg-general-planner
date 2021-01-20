package org.prebid.pg.gp.server.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.Json;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonUtilTest {

    @Test
    void shouldOptStringHandleNull() {
        String rs = JsonUtil.optString(null, "foo");
        assertThat(rs, is(nullValue()));
    }

    @Test
    void shouldOptStringHandleNonValueNode() throws Exception {
        ObjectNode objNode = Json.mapper.readValue("{\"foo\": []}", ObjectNode.class);
        assertThrows(IllegalArgumentException.class, () -> JsonUtil.optString(objNode, "foo"));
    }

    @Test
    void shouldSetValueHandleNull() {
        JsonUtil.setValue(null, "foo", "fooValue");
        assertThat(JsonUtil.optString(null, "foo"), is(nullValue()));

        JsonUtil.setValue(null, "foo", 5);
        assertThat(JsonUtil.optInt(null, "foo"), is(nullValue()));
    }

    @Test
    void shouldArrayItemAtHandleNull() {
        ObjectNode rs = JsonUtil.arrayItemAt(null, "foo", 0);
        assertThat(rs, is(nullValue()));
    }

    @Test
    void shouldArrayItemAtHandleNonArray() throws Exception {
        ObjectNode objNode = Json.mapper.readValue("{\"foo\": 5}", ObjectNode.class);
        ObjectNode rs = JsonUtil.arrayItemAt(objNode, "foo", 0);
        assertThat(rs, is(nullValue()));
    }
}
