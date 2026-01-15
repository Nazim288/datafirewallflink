package ru.gpbapp.datafirewallflink.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class FlatProfileDto {
    private final ObjectNode data;

    public FlatProfileDto(ObjectNode data) {
        this.data = data;
    }

    public ObjectNode json() {
        return data;
    }

    public JsonNode get(String key) {
        return key == null ? null : data.get(key);
    }

    public String getText(String key) {
        JsonNode n = get(key);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    /**
     * Представление профиля как Map<String, String> для Rule.apply(...).
     * Значения приводятся к строке:
     * - null/JSON null -> null
     * - числа/boolean -> asText()
     * - объекты/массивы -> toString() (JSON)
     */
    public Map<String, String> asStringMap() {
        Map<String, String> out = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = data.fields();

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            JsonNode v = e.getValue();

            if (key == null) continue;

            String str;
            if (v == null || v.isNull()) {
                str = null;
            } else if (v.isValueNode()) {
                // text/number/boolean
                str = v.asText();
            } else {
                // object/array -> JSON string
                str = v.toString();
            }

            out.put(key, str);
        }
        return out;
    }
}
