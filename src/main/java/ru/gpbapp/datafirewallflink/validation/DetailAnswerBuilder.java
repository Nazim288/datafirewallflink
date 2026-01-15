package ru.gpbapp.datafirewallflink.validation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Формирует JSON детального ответа (ANSWER_DETAIL).
 */
public final class DetailAnswerBuilder {

    private final ObjectMapper mapper;

    public DetailAnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param originalEvent   исходный event (QUERY)
     * @param allResult       итоговая агрегация (SUCCESS / ERROR)
     * @param detailByField  logicalField -> (ruleName -> SUCCESS|ERROR)
     */
    public ObjectNode buildDetailAnswer(JsonNode originalEvent,
                                        String allResult,
                                        Map<String, Map<String, String>> detailByField) {

        ObjectNode result = mapper.createObjectNode();

        // dataset → detail_results
        String dataset = getText(originalEvent, "dfw_dataset_code", "UNKNOWN_DATASET");

        ObjectNode detailResults = mapper.createObjectNode();
        ObjectNode datasetNode = mapper.createObjectNode();

        datasetNode.put("ALL_RESULT", allResult);

        for (Map.Entry<String, Map<String, String>> fieldEntry : detailByField.entrySet()) {
            ObjectNode rulesNode = mapper.createObjectNode();
            for (Map.Entry<String, String> ruleEntry : fieldEntry.getValue().entrySet()) {
                rulesNode.put(ruleEntry.getKey(), ruleEntry.getValue());
            }
            datasetNode.set(fieldEntry.getKey(), rulesNode);
        }

        detailResults.set(dataset, datasetNode);
        result.set("detail_results", detailResults);

        // --- meta поля ---
        copyIfExists(originalEvent, result, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm"
        ));

        String now = Instant.now().toString();
        result.put("dfw_action_type", "ANSWER_DETAIL");
        result.put("dfw_created_dttm", now);
        result.put("dfw_action_dttm", now);

        return result;
    }

    // ---------------- util ----------------

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        for (String f : fields) {
            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) {
                dst.set(f, v);
            }
        }
    }

    private static String getText(JsonNode node, String field, String def) {
        if (node == null) return def;
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? def : v.asText(def);
    }
}

