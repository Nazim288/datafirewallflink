package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.datafirewall.model.Rule;
import ru.gpbapp.datafirewallflink.validation.DetailsTemplateDynamic;
import ru.gpbapp.datafirewallflink.validation.FieldRuleBinding;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.*;

import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.ERROR;
import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.SUCCESS;

public final class ValidationService {

    private final DetailsTemplateDynamic template;

    public ValidationService(DetailsTemplateDynamic template) {
        this.template = template;
    }

    public ValidationResult validate(Map<String, Rule> rules,
                                     Map<String, String> flatData,
                                     List<FieldRuleBinding> bindings) {

        if (rules == null) rules = Map.of();
        if (flatData == null) flatData = Map.of();
        if (bindings == null) bindings = List.of();

        ObjectNode details = template.createDetailsSkeleton(bindings);

        boolean anyError = false;
        boolean anyException = false;

        Map<String, Map<String, String>> detail = new LinkedHashMap<>();

        try {
            List<FieldRuleBinding> sorted = new ArrayList<>(bindings);
            sorted.sort(Comparator.comparing(b -> b.ruleName));

            for (FieldRuleBinding b : sorted) {
                Rule rule = rules.get(b.ruleName);
                boolean ok;

                try {
                    ok = (rule != null) && rule.apply(flatData);
                } catch (Exception e) {
                    ok = false;
                    anyException = true;
                }

                String status = ok ? SUCCESS : ERROR;
                anyError |= !ok;

                writeStatus(details, b.section, b.field, status);

                detail.computeIfAbsent(b.logicalFieldKey, k -> new LinkedHashMap<>())
                        .put(b.ruleName, status);
            }

            String all = anyError ? ERROR : SUCCESS;
            details.put("ALL_RESULT", all);

            String processStatus = anyException ? "RULE_EXCEPTION" : "OK";
            return new ValidationResult(details, all, processStatus, deepUnmodifiable(detail));

        } catch (Exception fatal) {
            details.put("ALL_RESULT", ERROR);
            return new ValidationResult(details, ERROR, "ERROR", Map.of());
        }
    }

    private static Map<String, Map<String, String>> deepUnmodifiable(Map<String, Map<String, String>> m) {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        for (var e : m.entrySet()) {
            out.put(e.getKey(), Collections.unmodifiableMap(new LinkedHashMap<>(e.getValue())));
        }
        return Collections.unmodifiableMap(out);
    }

    private static void writeStatus(ObjectNode details, String section, String field, String status) {
        switch (section) {
            case "baseInfo" -> {
                JsonNode n = details.get("baseInfo");
                if (n instanceof ObjectNode baseInfo) baseInfo.put(field, status);
            }
            case "documents" -> {
                JsonNode n = details.get("documents");
                if (n instanceof ObjectNode documents) documents.put(field, status);
            }
            case "clientIdCard0" -> {
                JsonNode n = details.get("documents");
                if (!(n instanceof ObjectNode documents)) return;

                JsonNode arr = documents.get("clientIdCard");
                if (arr == null || !arr.isArray() || arr.isEmpty()) return;

                JsonNode first = arr.get(0);
                if (first instanceof ObjectNode card0) card0.put(field, status);
            }
            default -> { /* ignore */ }
        }
    }
}
