package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
     * @param detailByField   logicalField -> (ruleName -> SUCCESS|ERROR)
     */
    public ObjectNode buildDetailAnswer(JsonNode originalEvent,
                                        String allResult,
                                        Map<String, Map<String, String>> detailByField) {

        if (detailByField == null) detailByField = Map.of();
        if (allResult == null) allResult = "ERROR";

        ObjectNode result = mapper.createObjectNode();

        String dataset = getText(originalEvent, "dfw_dataset_code", "UNKNOWN_DATASET");

        ObjectNode detailResults = mapper.createObjectNode();
        ObjectNode datasetNode = mapper.createObjectNode();

        datasetNode.put("ALL_RESULT", allResult);

                // опционально: сортировка для стабильности
        Map<String, Map<String, String>> sortedFields =
                (detailByField instanceof java.util.SortedMap) ? detailByField : new TreeMap<>(detailByField);

        for (Map.Entry<String, Map<String, String>> fieldEntry : sortedFields.entrySet()) {
            String logicalField = fieldEntry.getKey();
            Map<String, String> ruleMap = fieldEntry.getValue();
            if (logicalField == null || logicalField.isBlank() || ruleMap == null) continue;

            ObjectNode rulesNode = mapper.createObjectNode();

            Map<String, String> sortedRules =
                    (ruleMap instanceof java.util.SortedMap) ? ruleMap : new TreeMap<>(ruleMap);

            for (Map.Entry<String, String> ruleEntry : sortedRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String status = ruleEntry.getValue();
                if (ruleName == null || ruleName.isBlank() || status == null) continue;
                rulesNode.put(ruleName, status);
            }

            datasetNode.set(logicalField, rulesNode);
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

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;

        for (String f : fields) {
            if (f == null) continue;
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
