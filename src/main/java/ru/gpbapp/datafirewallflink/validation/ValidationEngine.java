package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.gpbapp.datafirewallflink.rule.Rule;

import java.util.*;

import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.ERROR;
import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.SUCCESS;

public final class ValidationEngine {

    public static final class Result {
        public final ObjectNode details;      // details с SUCCESS/ERROR
        public final String allResult;        // SUCCESS|ERROR
        public final String processStatus;    // OK|ERROR (технический)

        public Result(ObjectNode details, String allResult, String processStatus) {
            this.details = details;
            this.allResult = allResult;
            this.processStatus = processStatus;
        }
    }

    /**
     * @param rules     текущие загруженные правила (registry.snapshot()).
     * @param data      flat Map<String,String> (из FlatProfileDto.asStringMap()).
     * @param bindings  куда писать результат каждого правила.
     * @param details   skeleton details (создаётся DetailsTemplateDynamic).
     */
    public Result validate(Map<String, Rule> rules,
                           Map<String, String> data,
                           List<FieldRuleBinding> bindings,
                           ObjectNode details) {

        boolean anyError = false;

        try {
            // детерминированно — удобно для отладки
            List<FieldRuleBinding> sorted = new ArrayList<>(bindings);
            sorted.sort(Comparator.comparing(b -> b.ruleName));

            for (FieldRuleBinding b : sorted) {
                Rule rule = rules.get(b.ruleName);

                boolean ok;
                try {
                    ok = (rule != null) && rule.apply(data);
                } catch (Exception e) {
                    ok = false;
                }

                String status = ok ? SUCCESS : ERROR;
                anyError |= !ok;

                writeStatus(details, b.section, b.field, status);
            }

            String all = anyError ? ERROR : SUCCESS;
            details.put("ALL_RESULT", all);

            return new Result(details, all, "OK");

        } catch (Exception fatal) {
            // если сам движок упал — это уже PROCESS_STATUS=ERROR
            details.put("ALL_RESULT", ERROR);
            return new Result(details, ERROR, "ERROR");
        }
    }

    /**
     * section:
     *  - "baseInfo"
     *  - "documents"
     *  - "clientIdCard0" (это documents.clientIdCard[0])
     */
    private static void writeStatus(ObjectNode details, String section, String field, String status) {
        switch (section) {
            case "baseInfo" -> {
                JsonNode n = details.get("baseInfo");
                if (n != null && n.isObject()) {
                    ((ObjectNode) n).put(field, status);
                }
            }
            case "documents" -> {
                JsonNode n = details.get("documents");
                if (n != null && n.isObject()) {
                    ((ObjectNode) n).put(field, status);
                }
            }
            case "clientIdCard0" -> {
                JsonNode docs = details.get("documents");
                if (docs == null || !docs.isObject()) return;

                JsonNode arr = docs.get("clientIdCard");
                if (arr == null || !arr.isArray() || arr.isEmpty()) return;

                JsonNode card0 = arr.get(0);
                if (card0 != null && card0.isObject()) {
                    ((ObjectNode) card0).put(field, status);
                }
            }
            default -> {
                // неизвестная секция — игнор
            }
        }
    }
}
