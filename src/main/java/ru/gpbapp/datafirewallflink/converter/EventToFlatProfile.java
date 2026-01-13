package ru.gpbapp.datafirewallflink.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

public final class EventToFlatProfile {

    private final ObjectMapper mapper;

    public EventToFlatProfile(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    private boolean isValidMapping(String mapping) {
        if (mapping == null) return false;

        String m = mapping.trim().toLowerCase();
        return !(m.isEmpty() || m.equals("none") || m.equals("null"));
    }

    public ObjectNode convert(JsonNode eventJson) {
        ObjectNode out = mapper.createObjectNode();

        JsonNode data = eventJson.path("data");
        if (data.isMissingNode() || data.isNull()) return out;

        // baseInfo
        projectSection(data.path("baseInfo"), out);

        // documents
        JsonNode documents = data.path("documents");
        projectSection(documents, out);

        // clientIdCard → выбираем primary=true, иначе первый
        JsonNode card = pickPrimary(documents.path("clientIdCard"));
        if (card != null) {
            projectSection(card, out);
        }

        return out;
    }

    private void projectSection(JsonNode section, ObjectNode out) {
        if (section == null || section.isMissingNode() || section.isNull() || !section.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> fields = section.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            String key = e.getKey();

            if (!key.startsWith("mapping.")) continue;

            String logical = key.substring("mapping.".length());
            JsonNode mappingNode = e.getValue();

            // mapping == null -> пропускаем
            if (mappingNode == null || mappingNode.isNull()) continue;

            String targetKeyRaw = mappingNode.asText();

            // trim + lower-case проверка: "", "none", "null" => пропускаем
            if (!isValidMapping(targetKeyRaw)) continue;

            String targetKey = targetKeyRaw.trim();

            JsonNode valueNode = section.get(logical);
            out.set(targetKey, valueNode == null ? mapper.nullNode() : valueNode);
        }
    }

    private JsonNode pickPrimary(JsonNode cards) {
        if (cards == null || !cards.isArray() || cards.isEmpty()) return null;

        for (JsonNode card : cards) {
            JsonNode primary = card.get("primary");
            if (primary != null && primary.isBoolean() && primary.booleanValue()) {
                return card;
            }
        }
        return cards.get(0);
    }
}
