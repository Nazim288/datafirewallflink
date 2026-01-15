package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.gpbapp.datafirewallflink.dto.FlatProfileDto;
import ru.gpbapp.datafirewallflink.rule.Rule;
import ru.gpbapp.datafirewallflink.validation.DetailsTemplateDynamic;
import ru.gpbapp.datafirewallflink.validation.FieldRuleBinding;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ShortAnswerCasesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    record Case(String inputResource, String expectedResource) {}

    static Stream<Case> cases() {
        return Stream.of(
                new Case("input/case1_normal.jsonl",         "answer/case1_short_expected.json"),
                new Case("input/case2_null_mapping.jsonl",   "answer/case2_short_expected.json"),
                new Case("input/case3_null_value.jsonl",     "answer/case3_short_expected.json"),
                new Case("input/case4_none_mapping.jsonl",   "answer/case4_short_expected.json"),
                new Case("input/case5_missing_fields.jsonl", "answer/case5_short_expected.json")
        );
    }

    @ParameterizedTest
    @MethodSource("cases")
    void should_build_short_answer(Case c) throws Exception {
        String inputLine = readResource(c.inputResource).trim();
        String expectedJson = readResource(c.expectedResource).trim();

        JsonNode original = MAPPER.readTree(inputLine);

        JsonEventProcessor processor = new JsonEventProcessor(new ObjectMapper());
        FlatProfileDto profile = processor.toFlatProfile(inputLine).orElseThrow();
        Map<String, String> flat = profile.asStringMap();

        // биндинги: ruleName + куда писать + logicalFieldKey (для detail, но пусть будет одинаково)
        List<FieldRuleBinding> bindings = List.of(
                new FieldRuleBinding("Rule1069", "baseInfo", "name", "ОСНОВНЫЕ СВЕДЕНИЯ.Имя"),
                new FieldRuleBinding("Rule1002", "baseInfo", "birthdate", "ОСНОВНЫЕ СВЕДЕНИЯ-Дата рождения"),
                new FieldRuleBinding("Rule1001", "documents", "clientSnils", "ОСНОВНЫЕ СВЕДЕНИЯ, СНИЛС")
        );

        // тестовые правила (можешь заменить на реальные Rule1069 и т.д., если они доступны в тестах)
        Map<String, Rule> rules = Map.of(
                "Rule1069", nameContainsCyrAndLat("ОСНОВНЫЕ СВЕДЕНИЯ.Имя"), // как твой пример
                "Rule1002", requiredNotMasked("ОСНОВНЫЕ СВЕДЕНИЯ-Дата рождения"),
                "Rule1001", requiredNotMasked("ОСНОВНЫЕ СВЕДЕНИЯ, СНИЛС")
        );

        DetailsTemplateDynamic template = new DetailsTemplateDynamic(MAPPER);
        ValidationService validationService = new ValidationService(template);
        ValidationResult vr = validationService.validate(rules, flat, bindings);

        ShortAnswerService shortService = new ShortAnswerService(MAPPER);
        String actual = shortService.build(original, vr);

        System.out.println("=== ACTUAL SHORT: " + c.inputResource + " ===");
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(MAPPER.readTree(actual)));

        ObjectNode actualNode = (ObjectNode) MAPPER.readTree(actual);
        stripShortDynamicFields(actualNode);

        JsonNode expectedNode = MAPPER.readTree(expectedJson);
        assertThat(actualNode).isEqualTo(expectedNode);
    }

    // --- helpers ---

    private static Rule requiredNotMasked(String key) {
        return data -> {
            String v = data.get(key);
            if (v == null) return false;
            String s = v.trim();
            if (s.isEmpty()) return false;

            boolean allStars = true;
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) != '*') { allStars = false; break; }
            }
            return !allStars;
        };
    }

    private static Rule nameContainsCyrAndLat(String key) {
        return data -> {
            String s = data.get(key);
            if (s == null) return false;
            boolean hasLat = s.matches(".*[a-zA-Z]+.*");
            boolean hasCyr = s.matches(".*[а-яА-ЯеЁ]+.*");
            return hasLat && hasCyr;
        };
    }

    private static void stripShortDynamicFields(ObjectNode n) {
        n.remove("dfw_created_dttm");
        n.remove("dfw_action_dttm");
    }

    private static String readResource(String classpathPath) throws Exception {
        try (InputStream is = ShortAnswerCasesTest.class.getClassLoader().getResourceAsStream(classpathPath)) {
            if (is == null) throw new IllegalStateException("Resource not found: " + classpathPath);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}

