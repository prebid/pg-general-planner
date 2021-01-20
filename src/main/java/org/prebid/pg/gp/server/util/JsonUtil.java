package org.prebid.pg.gp.server.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.util.StringUtils;

import java.util.function.Function;

/**
 * Utility class for handling Json nodes.
 */
public final class JsonUtil {

    private JsonUtil() {
    }

    public static String optString(ObjectNode objectNode, String field) {
        if (objectNode == null) {
            return null;
        }
        JsonNode fieldNode = objectNode.get(field);
        if (fieldNode == null) {
            return null;
        }
        if (fieldNode.isValueNode()) {
            return fieldNode.asText();
        }
        throw new IllegalArgumentException(field + "is not a value field.");
    }

    public static Integer optInt(ObjectNode objectNode, String field) {
        String val = optString(objectNode, field);
        return StringUtils.isEmpty(val) ? null : Integer.valueOf(val);
    }

    public static void setValue(ObjectNode objectNode, String field, String value) {
        if (objectNode == null) {
            return;
        }
        objectNode.put(field, value);
    }

    public static void setValue(ObjectNode objectNode, String field, int value) {
        if (objectNode == null) {
            return;
        }
        objectNode.put(field, value);
    }

    public static void updateArrayItemField(ObjectNode objectNode, String field, String itemField,
            Function<String, String> mapping) {
        if (objectNode == null) {
            return;
        }
        JsonNode fieldNode = objectNode.get(field);
        if (!(fieldNode instanceof ArrayNode)) {
            return;
        }
        for (JsonNode itemNode : fieldNode) {
            if (itemNode instanceof ObjectNode) {
                ObjectNode itemObjNode = (ObjectNode) itemNode;
                String newValue = mapping.apply(optString(itemObjNode, itemField));
                setValue(itemObjNode, itemField, newValue);
            }
        }
    }

    public static ObjectNode arrayItemAt(ObjectNode objectNode, String field, int index) {
        if (objectNode == null) {
            return null;
        }
        JsonNode fieldNode = objectNode.get(field);
        if (fieldNode instanceof ArrayNode) {
            ArrayNode array = (ArrayNode) fieldNode;
            JsonNode item = array.get(index);
            if (item instanceof ObjectNode) {
                return (ObjectNode) item;
            }
        }
        return null;
    }
}
