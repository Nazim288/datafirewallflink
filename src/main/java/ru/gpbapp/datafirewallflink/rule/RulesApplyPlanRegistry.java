// RulesApplyPlanRegistry.java
package ru.gpbapp.datafirewallflink.rule;

import ru.gpbapp.datafirewallflink.validation.FieldRuleBinding;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * dataset -> список биндингов (какое правило к какому полю применять)
 */
public final class RulesApplyPlanRegistry {

    public static final String DEFAULT_KEY = "*";

    private final AtomicReference<Map<String, List<FieldRuleBinding>>> ref =
            new AtomicReference<>(Map.of());

    /** immutable snapshot */
    public Map<String, List<FieldRuleBinding>> snapshot() {
        return ref.get();
    }

    /** atomic swap */
    public void replaceAll(Map<String, List<FieldRuleBinding>> newPlan) {
        Objects.requireNonNull(newPlan, "newPlan must not be null");

        Map<String, List<FieldRuleBinding>> copy = new LinkedHashMap<>();
        for (var e : newPlan.entrySet()) {
            copy.put(e.getKey(), List.copyOf(e.getValue()));
        }
        ref.set(Collections.unmodifiableMap(copy));
    }

    /** план для конкретного dataset */
    public List<FieldRuleBinding> getPlan(String dataset) {
        if (dataset == null) return defaultPlan();
        Map<String, List<FieldRuleBinding>> m = ref.get();
        List<FieldRuleBinding> p = m.get(dataset);
        return (p != null) ? p : defaultPlan();
    }

    /** default план */
    public List<FieldRuleBinding> defaultPlan() {
        Map<String, List<FieldRuleBinding>> m = ref.get();
        return m.getOrDefault(DEFAULT_KEY, List.of());
    }

    public void clear() {
        ref.set(Map.of());
    }

    /**
     * !!!!!!!!!! ВРЕМЕННО ДЛЯ ТЕСТА !!!!!!!!!!
     * Все 54 правила применяем к baseInfo.fullName
     * logicalFieldKey = "ОСНОВНЫЕ СВЕДЕНИЯ,ФИО одной строкой"
     */
    public void buildRules() {
        String logicalFieldKey = "ОСНОВНЫЕ СВЕДЕНИЯ,ФИО одной строкой";

        List<String> ruleNames = List.of(
                "Rule10248", "Rule10083", "Rule1106", "Rule1227", "Rule1148", "Rule10088",
                "Rule1109", "Rule10200", "Rule1108", "Rule1229", "Rule1231", "Rule10080",
                "Rule1113", "Rule1234", "Rule10081", "Rule1072", "Rule1192", "Rule10218",
                "Rule1118", "Rule1117", "Rule1115", "Rule1119", "Rule1121", "Rule1087",
                "Rule1120", "Rule1164", "Rule1125", "Rule1124", "Rule1080", "Rule1161",
                "Rule10106", "Rule1248", "Rule1126", "Rule1099", "Rule1253", "Rule1097",
                "Rule1130", "Rule1250", "Rule1257", "Rule1255", "Rule1092", "Rule10072",
                "Rule1338", "Rule1259", "Rule10075", "Rule1137", "Rule1258", "Rule10076",
                "Rule10077", "Rule10078", "Rule10079", "Rule1066", "Rule1103", "Rule1146"
        );

        List<FieldRuleBinding> plan = new ArrayList<>(ruleNames.size());
        for (String rn : ruleNames) {
            plan.add(new FieldRuleBinding(
                    rn,            // имя rule в rulesRegistry
                    "baseInfo",     // section (куда писать статус в details)
                    "fullName",     // field (внутри section)
                    logicalFieldKey // ключ в normalizedMap
            ));
        }
        plan.sort(Comparator.comparing(b -> b.ruleName));

        Map<String, List<FieldRuleBinding>> m = new LinkedHashMap<>();
        m.put(DEFAULT_KEY, plan);
        m.put("Дашборд.Тесса", plan);

        replaceAll(m);
    }
}
