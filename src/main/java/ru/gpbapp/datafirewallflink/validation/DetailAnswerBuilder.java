package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Формирует JSON детального ответа (ANSWER_DETAIL) в формате как у тебя в эталоне:
 * detail_results: {
 *   "Дашборд.УС ЛИК": { ... },
 *   "УС.ЛиК.Адрес проживания": { ... },
 *   "УС.ЛиК.Адрес регистрации": { ... }
 * }
 *
 * rule keys: "Rule1099" -> "1099"
 */
public final class DetailAnswerBuilder {

    private static final Pattern RULE_NUM = Pattern.compile("(?i)^Rule(\\d+)$");

    private final ObjectMapper mapper;

    public DetailAnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildDetailAnswer(JsonNode originalEvent,
                                        String allResult,
                                        Map<String, Map<String, String>> detailByField) {

        if (detailByField == null) detailByField = Map.of();
        if (allResult == null) allResult = "ERROR";

        ObjectNode result = mapper.createObjectNode();

        String mainDataset = getText(originalEvent, "dfw_dataset_code", "UNKNOWN_DATASET");

        JsonNode data = originalEvent == null ? null : originalEvent.get("data");
        String homeDs = getText(data == null ? null : data.get("homeAddress"), "dataset_code", "УС.ЛиК.Адрес проживания");
        String regDs  = getText(data == null ? null : data.get("registrationAddress"), "dataset_code", "УС.ЛиК.Адрес регистрации");

        // какие logicalField относятся к адресам — берём из mapping.* внутри этих объектов
        Set<String> homeAddressLogical = collectMappingLogicalFields(data == null ? null : data.get("homeAddress"));
        Set<String> regAddressLogical  = collectMappingLogicalFields(data == null ? null : data.get("registrationAddress"));

        Map<String, ObjectNode> buckets = new LinkedHashMap<>();
        buckets.put(mainDataset, mapper.createObjectNode());
        buckets.put(homeDs, mapper.createObjectNode());
        buckets.put(regDs, mapper.createObjectNode());

        Map<String, Boolean> hasError = new HashMap<>();
        for (String ds : buckets.keySet()) hasError.put(ds, false);

        Map<String, Map<String, String>> sortedFields =
                (detailByField instanceof SortedMap) ? detailByField : new TreeMap<>(detailByField);

        for (Map.Entry<String, Map<String, String>> fieldEntry : sortedFields.entrySet()) {

            String logicalField = fieldEntry.getKey();
            Map<String, String> ruleMap = fieldEntry.getValue();
            if (logicalField == null || logicalField.isBlank() || ruleMap == null || ruleMap.isEmpty()) continue;

            boolean isHome = homeAddressLogical.contains(logicalField);
            boolean isReg  = regAddressLogical.contains(logicalField);

            List<String> targetDatasets = new ArrayList<>();
            if (isHome) targetDatasets.add(homeDs);
            if (isReg)  targetDatasets.add(regDs);
            if (targetDatasets.isEmpty()) targetDatasets.add(mainDataset);

            ObjectNode rulesNode = mapper.createObjectNode();

            Map<String, String> sortedRules =
                    (ruleMap instanceof SortedMap) ? ruleMap : new TreeMap<>(ruleMap);

            boolean fieldHasError = false;
            for (Map.Entry<String, String> ruleEntry : sortedRules.entrySet()) {
                String ruleName = ruleEntry.getKey();
                String status = ruleEntry.getValue();
                if (ruleName == null || ruleName.isBlank() || status == null) continue;

                String normRuleKey = normalizeRuleKey(ruleName);
                rulesNode.put(normRuleKey, status);

                if ("ERROR".equalsIgnoreCase(status)) fieldHasError = true;
            }

            for (String ds : targetDatasets) {
                ObjectNode dsNode = buckets.get(ds);
                if (dsNode == null) continue;
                dsNode.set(logicalField, rulesNode);
                if (fieldHasError) hasError.put(ds, true);
            }
        }

        // проставим ALL_RESULT на каждом dataset уровне
        for (Map.Entry<String, ObjectNode> e : buckets.entrySet()) {
            String ds = e.getKey();
            ObjectNode dsNode = e.getValue();
            dsNode.put("ALL_RESULT", hasError.getOrDefault(ds, false) ? "ERROR" : "SUCCESS");
        }

        ObjectNode detailResults = mapper.createObjectNode();
        for (Map.Entry<String, ObjectNode> e : buckets.entrySet()) {
            detailResults.set(e.getKey(), e.getValue());
        }

        result.set("detail_results", detailResults);

        // --- meta поля ---
        copyIfExists(originalEvent, result, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm",
                "dfw_created_dttm"
        ));

        String now = Instant.now().toString();
        result.put("dfw_action_type", "ANSWER_DETAIL");
        result.put("dfw_action_dttm", now);
        if (result.get("dfw_created_dttm") == null) {
            result.put("dfw_created_dttm", now);
        }

        return result;
    }

    private static String normalizeRuleKey(String ruleName) {
        Matcher m = RULE_NUM.matcher(ruleName.trim());
        if (m.matches()) return m.group(1);
        return ruleName.trim();
    }

    private static Set<String> collectMappingLogicalFields(JsonNode node) {
        Set<String> out = new HashSet<>();
        if (node == null || node.isNull() || !node.isObject()) return out;

        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String k = e.getKey();
            JsonNode v = e.getValue();
            if (k == null || v == null || v.isNull()) continue;

            //все поля вида mapping.xxx : "ЛОГИЧЕСКОЕ.ИМЯ"
            if (k.startsWith("mapping.")) {
                String logical = v.asText(null);
                if (logical != null && !logical.isBlank() && !"none".equalsIgnoreCase(logical)) {
                    out.add(logical);
                }
            }
        }
        return out;
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;
        for (String f : fields) {
            if (f == null) continue;
            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) dst.set(f, v);
        }
    }

    private static String getText(JsonNode node, String field, String def) {
        if (node == null) return def;
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? def : v.asText(def);
    }
}