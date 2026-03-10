package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class AnswerBuilder {

    private final ObjectMapper mapper;

    public AnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildAnswer(JsonNode originalEvent, ValidationResult validation) {

        ObjectNode out = mapper.createObjectNode();
        copyIfExists(originalEvent, out, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm",
                "dfw_created_dttm"
        ));

        out.put("dfw_action_type", "ANSWER");

        String now = Instant.now().toString();
        out.put("dfw_readed_buf_dttm", now);
        out.put("dfw_sending_to_mq_dttm", now);
        out.put("dfw_action_dttm", now);

        if (out.get("dfw_created_dttm") == null) {
            out.put("dfw_created_dttm", now);
        }

        String processStatus = (validation == null || validation.processStatus() == null) ? "ERROR" : validation.processStatus();
        out.put("PROCESS_STATUS", processStatus);

        out.set("details", buildShortDetails(originalEvent, validation));
        return out;
    }

    private ObjectNode buildShortDetails(JsonNode originalEvent, ValidationResult validation) {
        ObjectNode details = mapper.createObjectNode();

        String all = (validation == null || validation.allResult() == null) ? "ERROR" : validation.allResult();
        details.put("ALL_RESULT", all);

        Map<String, Map<String, String>> detailByField =
                (validation == null || validation.detailByField() == null) ? Map.of() : validation.detailByField();

        JsonNode data = (originalEvent == null) ? null : originalEvent.get("data");

        details.set("homeAddress", buildAddressShortNode(
                data == null ? null : data.get("homeAddress"),
                detailByField,
                "УС.ЛиК.Адрес проживания"
        ));

        details.set("registrationAddress", buildAddressShortNode(
                data == null ? null : data.get("registrationAddress"),
                detailByField,
                "УС.ЛиК.Адрес регистрации"
        ));

        details.set("contactInfo", buildContactShortNode(
                data == null ? null : data.get("contactInfo"),
                detailByField
        ));

        details.set("baseInfo", buildBaseInfoShortNode(
                data == null ? null : data.get("baseInfo"),
                detailByField
        ));

        details.set("documents", buildDocumentsShortNode(
                data == null ? null : data.get("documents"),
                detailByField
        ));

        return details;
    }

    private ObjectNode buildAddressShortNode(JsonNode addr, Map<String, Map<String, String>> detailByField, String fallbackDataset) {
        ObjectNode o = mapper.createObjectNode();
        o.put("dataset_code", text(addr, "dataset_code", fallbackDataset));

        o.put("city", statusByMappingFlexible(addr, "mapping.city", detailByField));
        o.put("countryCode", statusByMappingFlexible(addr, "mapping.countryCode", detailByField));
        o.put("postalCode", statusByMappingFlexible(addr, "mapping.postalCode", detailByField));
        o.put("street", statusByMappingFlexible(addr, "mapping.street", detailByField));
        o.put("area", statusByMappingFlexible(addr, "mapping.area", detailByField));
        o.put("countryName", statusByMappingFlexible(addr, "mapping.countryName", detailByField));
        o.put("settlement", statusByMappingFlexible(addr, "mapping.settlement", detailByField));

        return o;
    }

    private ObjectNode buildContactShortNode(JsonNode contact, Map<String, Map<String, String>> detailByField) {
        ObjectNode o = mapper.createObjectNode();
        o.put("dataset_code", text(contact, "dataset_code", "УС.ЛиК.Контакты клиента"));

        o.put("mobilePhone", statusByMappingFlexible(contact, "mapping.mobilePhone", detailByField));
        o.put("emailValue", statusByMappingFlexible(contact, "mapping.emailValue", detailByField));

        return o;
    }

    private ObjectNode buildBaseInfoShortNode(JsonNode base, Map<String, Map<String, String>> detailByField) {
        ObjectNode o = mapper.createObjectNode();
        o.put("dataset_code", text(base, "dataset_code", "УС.ЛиК.Данные клиента"));

        o.put("citizenship", statusByMappingFlexible(base, "mapping.citizenship", detailByField));
        o.put("birthPlace", statusByMappingFlexible(base, "mapping.birthPlace", detailByField));
        o.put("surname", statusByMappingFlexible(base, "mapping.surname", detailByField));
        o.put("name", statusByMappingFlexible(base, "mapping.name", detailByField));
        o.put("gender", statusByMappingFlexible(base, "mapping.gender", detailByField));
        o.put("fullName", statusByMappingFlexible(base, "mapping.fullName", detailByField));
        o.put("birthdate", statusByMappingFlexible(base, "mapping.birthdate", detailByField));
        o.put("patronymic", statusByMappingFlexible(base, "mapping.patronymic", detailByField));

        return o;
    }

    private ObjectNode buildDocumentsShortNode(JsonNode docs, Map<String, Map<String, String>> detailByField) {
        ObjectNode o = mapper.createObjectNode();
        o.put("dataset_code", "УС.ЛиК.Документы клиента");

        o.put("clientInn", statusByMappingFlexible(docs, "mapping.clientInn", detailByField));
        o.put("clientSnils", statusByMappingFlexible(docs, "mapping.clientSnils", detailByField));

        ArrayNode arr = mapper.createArrayNode();
        JsonNode cards = docs == null ? null : docs.get("clientIdCard");
        if (cards != null && cards.isArray() && cards.size() > 0) {
            JsonNode card0 = cards.get(0);
            ObjectNode c = mapper.createObjectNode();

            c.put("elemId", text(card0, "elemId", text(card0, "type", "UNKNOWN")));

            c.put("issueAuthority", statusByMappingFlexible(card0, "mapping.issueAuthority", detailByField));
            c.put("number", statusByMappingFlexible(card0, "mapping.number", detailByField));
            c.put("issueDate", statusByMappingFlexible(card0, "mapping.issueDate", detailByField));
            c.put("departmentCode", statusByMappingFlexible(card0, "mapping.departmentCode", detailByField));
            c.put("series", statusByMappingFlexible(card0, "mapping.series", detailByField));

            arr.add(c);
        }
        o.set("clientIdCard", arr);

        return o;
    }

    /**
     * Ищем logicalField в validation.detailByField() в двух вариантах:
     *  1) как в mapping: "ОСНОВНЫЕ СВЕДЕНИЯ.ФИО одной строкой"
     *  2) как в правилах/детальном: "ОСНОВНЫЕ СВЕДЕНИЯ,ФИО одной строкой"
     */
    private String statusByMappingFlexible(JsonNode node, String mappingKey, Map<String, Map<String, String>> detailByField) {
        String logical = text(node, mappingKey, null);
        if (logical == null || logical.isBlank() || "none".equalsIgnoreCase(logical)) return "ERROR";

        Map<String, String> rules = detailByField.get(logical);
        if (rules == null) {
            String alt = logical.replace('.', ',');
            rules = detailByField.get(alt);
        }

        return aggregateFieldStatus(rules);
    }

    private String aggregateFieldStatus(Map<String, String> rules) {
        if (rules == null || rules.isEmpty()) return "ERROR";
        for (String v : rules.values()) {
            if (v == null) continue;
            if ("ERROR".equalsIgnoreCase(v)) return "ERROR";
        }
        return "SUCCESS";
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        if (src == null || dst == null || fields == null) return;
        for (String f : fields) {
            if (f == null) continue;
            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) dst.set(f, v);
        }
    }

    private static String text(JsonNode node, String field, String def) {
        if (node == null || field == null) return def;
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? def : v.asText(def);
    }
}