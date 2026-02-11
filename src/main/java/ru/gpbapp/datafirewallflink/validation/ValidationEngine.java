package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.model.Rule;

import java.util.*;

import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.ERROR;
import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.SUCCESS;

public final class ValidationEngine {

    public static final class Result {
        public final ObjectNode details;      // details с SUCCESS/ERROR
        public final String allResult;        // SUCCESS|ERROR
        public final String processStatus;    // OK|ERROR

        public Result(ObjectNode details, String allResult, String processStatus) {
            this.details = details;
            this.allResult = allResult;
            this.processStatus = processStatus;
        }
    }

    public Result validate(Map<String, Rule> rules,
                           Map<String, String> data,
                           List<FieldRuleBinding> bindings,
                           ObjectNode details) {

        if (rules == null) rules = Map.of();
        if (data == null) data = Map.of();
        if (bindings == null) bindings = List.of();
        if (details == null) {
            throw new IllegalArgumentException("details skeleton must not be null");
        }

        boolean anyError = false;

        try {
            List<FieldRuleBinding> sorted = new ArrayList<>(bindings);
            sorted.sort(Comparator.comparing(b -> b.ruleName));

            for (FieldRuleBinding b : sorted) {
                if (b == null) continue;

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
            details.put("ALL_RESULT", ERROR);
            return new Result(details, ERROR, "ERROR");
        }
    }

    private static void writeStatus(ObjectNode details, String section, String field, String status) {
        if (details == null || section == null || field == null || status == null) return;

        switch (section) {
            case "baseInfo" -> {
                JsonNode n = details.get("baseInfo");
                if (n instanceof ObjectNode baseInfo) {
                    baseInfo.put(field, status);
                }
            }
            case "documents" -> {
                JsonNode n = details.get("documents");
                if (n instanceof ObjectNode documents) {
                    documents.put(field, status);
                }
            }
            case "clientIdCard0" -> {
                JsonNode docs = details.get("documents");
                if (!(docs instanceof ObjectNode documents)) return;

                JsonNode arr = documents.get("clientIdCard");
                if (arr == null || !arr.isArray() || arr.isEmpty()) return;

                JsonNode card0 = arr.get(0);
                if (card0 instanceof ObjectNode obj) {
                    obj.put(field, status);
                }
            }
            default -> { /* ignore */ }
        }
    }
}
