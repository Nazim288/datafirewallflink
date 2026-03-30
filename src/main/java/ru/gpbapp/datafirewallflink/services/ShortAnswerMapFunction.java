package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.model.Rule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.IgniteRulesApiClient;
import ru.gpbapp.datafirewallflink.converter.MappingNormalizer;
import ru.gpbapp.datafirewallflink.dto.HttpBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.dto.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.cache.CompiledRulesRegistry;
import ru.gpbapp.datafirewallflink.rule.RulesReloader;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class ShortAnswerMapFunction extends RichMapFunction<MqRecord, MqReply> {

    private static final Logger log = LoggerFactory.getLogger(ShortAnswerMapFunction.class);

    private transient ObjectMapper mapper;

    private transient CompiledRulesRegistry rulesRegistry;
    private transient RulesReloader reloader;
    private transient BytecodeSource bytecodeSource;
    private transient AutoCloseable closeable;

    private transient ValidationService validationService;
    private transient ShortAnswerService shortAnswerService;
    private transient DetailAnswerService detailAnswerService;

    private transient MappingNormalizer normalizer;

    @Override
    public void open(Configuration parameters) {
        this.rulesRegistry = new CompiledRulesRegistry();

        ParameterTool pt = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();

        String mode = pt != null
                ? pt.get("rules.loader", "http").toLowerCase(Locale.ROOT).trim()
                : "http";

        String sourceName = pt != null ? pt.get("rules.sourceName", "my-source") : "my-source";
        String cacheName = pt != null ? pt.get("ignite.cache", "compiled_" + sourceName) : ("compiled_" + sourceName);
        String nameToLoad = "http".equals(mode) ? sourceName : cacheName;

        BytecodeSource rawSource;

        if ("http".equals(mode)) {
            String igniteApiUrl = pt != null ? pt.get("ignite.apiUrl", "http://127.0.0.1:8080") : "http://127.0.0.1:8080";
            IgniteRulesApiClient apiClient = new IgniteRulesApiClient(igniteApiUrl);
            rawSource = new HttpBytecodeSource(apiClient);
            this.closeable = null;
            log.info("[RULES] loader=http apiUrl={} sourceName={}", igniteApiUrl, sourceName);

        } else if ("thin".equals(mode)) {
            String igniteHost = pt != null ? pt.get("ignite.host", "127.0.0.1") : "127.0.0.1";
            int ignitePort = pt != null ? pt.getInt("ignite.port", 10800) : 10800;

            IgniteClientFacadeImpl ignite = new IgniteClientFacadeImpl(igniteHost, ignitePort);
            IgniteClientFacade facade = ignite;

            rawSource = new IgniteBytecodeSource(facade);
            this.closeable = ignite;

            log.info("[RULES] loader=thin host={} port={} cacheName={}", igniteHost, ignitePort, cacheName);

        } else {
            throw new IllegalArgumentException("Unknown rules.loader=" + mode + " (use thin|http)");
        }

        this.mapper = new ObjectMapper();

        this.bytecodeSource = new TimedBytecodeSource(rawSource, msg -> log.info(msg));
        this.reloader = new RulesReloader(bytecodeSource, rulesRegistry);

        long t0 = System.nanoTime();
        reloader.reloadAllStrict(nameToLoad);
        long ms = (System.nanoTime() - t0) / 1_000_000;

        Map<String, Rule> rules = rulesRegistry.snapshot();

        log.info("[RULES_REGISTRY] loadedRulesCount={}", rules.size());

        rules.keySet().stream()
                .sorted()
                .forEach(k -> log.info("[RULES_REGISTRY] ruleKey={}", k));

        this.validationService = new ValidationService();
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);

        this.normalizer = new MappingNormalizer(mapper);

        log.info("[INIT] subtask={}", getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public MqReply map(MqRecord in) {
        if (in == null || in.payload == null || in.payload.isBlank()) {
            log.warn("[PIPE][no-qid] Empty MQ payload");
            return null;
        }

        String raw = in.payload;

        try {
            JsonNode originalEvent = mapper.readTree(raw);
            String qid = originalEvent.path("dfw_query_id").asText("no-qid");

            log.info("[PIPE][{}] 1) MQ_IN raw(masked):\n{}", qid, maskJsonPretty(raw));

            // normalizedMap из mapping.* по всему event.data
            Map<String, String> normalizedMap = normalizer.normalize(originalEvent);

            log.info("[PIPE][{}] 2) NORMALIZED_MAP size={}", qid, normalizedMap.size());
            log.info("[PIPE][{}] 2) NORMALIZED_MAP full(masked):\n{}",
                    qid,
                    prettyMapAsJson(maskMap(normalizedMap)));

            Map<String, Rule> compiledRules = rulesRegistry.snapshot();

            //применяем RulePlan.FIELD_TO_RULES
            ValidationResult validation = validationService.validate(
                    compiledRules,
                    normalizedMap,
                    ru.gpbapp.datafirewallflink.rule.RulePlan.FIELD_TO_RULES
            );

            String shortJson = shortAnswerService.build(originalEvent, validation);
            if (shortJson == null) {
                log.warn("[PIPE][{}] ShortAnswerService returned null.", qid);
                return null;
            }
            log.info("[PIPE][{}] 3) ANSWER_SHORT (masked):\n{}", qid, maskJsonPretty(shortJson));

            String detailJson = detailAnswerService.build(originalEvent, validation);
            if (detailJson == null) {
                log.warn("[PIPE][{}] DetailAnswerService returned null.", qid);
            } else {
                log.info("[PIPE][{}] 4) ANSWER_DETAIL (masked):\n{}", qid, maskJsonPretty(detailJson));
            }

            return new MqReply(in.msgId, shortJson);

        } catch (Exception e) {
            log.error("Failed to build answers.", e);
            return null;
        }
    }

    @Override
    public void close() {
        try {
            if (closeable != null) closeable.close();
        } catch (Exception e) {
            log.warn("Failed to close resources", e);
        }
    }

    // ---------------- helpers: pretty / masking ----------------

    private String prettyMapAsJson(Map<String, String> m) {
        if (m == null) return "null";
        try {
            Map<String, String> sorted = new TreeMap<>(m);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sorted);
        } catch (Exception e) {
            return m.toString();
        }
    }

    private String maskJsonPretty(String json) {
        if (json == null || json.isBlank()) return json;
        try {
            JsonNode root = mapper.readTree(json);
            maskNode(root);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return json;
        }
    }

    private void maskNode(JsonNode node) {
        if (node == null) return;
        if (node.isObject()) {
            Iterator<String> it = node.fieldNames();
            while (it.hasNext()) {
                String fn = it.next();
                JsonNode child = node.get(fn);

                if (isSensitiveKey(fn) && node instanceof com.fasterxml.jackson.databind.node.ObjectNode obj) {
                    obj.put(fn, "***");
                } else {
                    maskNode(child);
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) maskNode(child);
        }
    }

    private boolean isSensitiveKey(String key) {
        if (key == null) return false;
        String k = key.toLowerCase(Locale.ROOT);
        return k.equals("birthdate")
                || k.equals("clientsnils")
                || k.equals("snils")
                || k.equals("inn")
                || k.equals("number")
                || k.equals("series")
                || k.equals("departmentcode");
    }

    private Map<String, String> maskMap(Map<String, String> m) {
        if (m == null) return null;
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();

            if (k != null && (isSensitiveKey(k)
                    || k.toLowerCase(Locale.ROOT).contains("snils")
                    || k.toLowerCase(Locale.ROOT).contains("birthdate")
                    || k.toLowerCase(Locale.ROOT).contains("passport")
                    || k.toLowerCase(Locale.ROOT).contains("number"))) {
                out.put(k, "***");
            } else {
                out.put(k, v);
            }
        }
        return out;
    }
}