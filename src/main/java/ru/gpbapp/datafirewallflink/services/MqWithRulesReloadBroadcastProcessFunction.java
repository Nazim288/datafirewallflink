package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.datafirewall.model.Rule;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.IgniteRulesApiClient;
import ru.gpbapp.datafirewallflink.dto.FlatProfileDto;
import ru.gpbapp.datafirewallflink.dto.HttpBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.kafka.RulesVersionEvent;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.rule.CompiledRulesRegistry;
import ru.gpbapp.datafirewallflink.rule.RulesApplyPlanRegistry;
import ru.gpbapp.datafirewallflink.rule.RulesReloader;
import ru.gpbapp.datafirewallflink.validation.DetailsTemplateDynamic;
import ru.gpbapp.datafirewallflink.validation.FieldRuleBinding;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.*;

public class MqWithRulesReloadBroadcastProcessFunction
        extends BroadcastProcessFunction<MqRecord, RulesVersionEvent, MqReply> {

    private static final Logger log = LoggerFactory.getLogger(MqWithRulesReloadBroadcastProcessFunction.class);

    private static final String KEY_VERSION = "version";

    private final MapStateDescriptor<String, Long> rulesBroadcastDesc;

    private transient ObjectMapper mapper;
    private transient JsonEventProcessor processor;

    private transient CompiledRulesRegistry rulesRegistry;
    private transient RulesReloader reloader;
    private transient BytecodeSource bytecodeSource;

    private transient AutoCloseable closeable;

    private transient DetailsTemplateDynamic detailsTemplate;
    private transient ValidationService validationService;
    private transient ShortAnswerService shortAnswerService;
    private transient DetailAnswerService detailAnswerService;

    private transient RulesApplyPlanRegistry planRegistry;

    private transient String nameToLoad;

    private transient boolean logPayloads;
    private transient int logPreviewLen;

    public MqWithRulesReloadBroadcastProcessFunction(MapStateDescriptor<String, Long> rulesBroadcastDesc) {
        this.rulesBroadcastDesc = rulesBroadcastDesc;
    }

    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();

        ParameterTool pt = (ParameterTool) rc.getExecutionConfig().getGlobalJobParameters();
        if (pt == null) pt = ParameterTool.fromMap(Map.of());

        this.logPayloads = pt.getBoolean("log.payloads", false);
        this.logPreviewLen = pt.getInt("log.preview.len", 600);

        this.rulesRegistry = new CompiledRulesRegistry();

        initRulesLoaderAndLoad(pt);

        this.mapper = new ObjectMapper();
        this.processor = new JsonEventProcessor(mapper);

        this.detailsTemplate = new DetailsTemplateDynamic(mapper);
        this.validationService = new ValidationService(detailsTemplate);
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);

        this.planRegistry = new RulesApplyPlanRegistry();

                // тестовый план !!!!! временно
        this.planRegistry.buildRules();

        log.info("[INIT] subtask={} log.payloads={} log.preview.len={} rulesLoaded={} planSize={}",
                rc.getIndexOfThisSubtask(),
                logPayloads,
                logPreviewLen,
                rulesRegistry.snapshot().size(),
                planRegistry.snapshot().size()
        );
    }

    private void initRulesLoaderAndLoad(ParameterTool pt) {
        String mode = pt.get("rules.loader", "http").toLowerCase(Locale.ROOT).trim();

        String sourceName = pt.get("rules.sourceName", "my-source");
        String cacheName = pt.get("ignite.cache", "compiled_" + sourceName);
        this.nameToLoad = "http".equals(mode) ? sourceName : cacheName;

        BytecodeSource rawSource;

        if ("http".equals(mode)) {
            String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
            IgniteRulesApiClient apiClient = new IgniteRulesApiClient(igniteApiUrl);
            rawSource = new HttpBytecodeSource(apiClient);
            this.closeable = null;
            log.info("[RULES] loader=http apiUrl={} sourceName={}", igniteApiUrl, sourceName);

        } else if ("thin".equals(mode)) {
            String igniteHost = pt.get("ignite.host", "127.0.0.1");
            int ignitePort = pt.getInt("ignite.port", 10800);
            IgniteClientFacadeImpl ignite = new IgniteClientFacadeImpl(igniteHost, ignitePort);
            IgniteClientFacade facade = ignite;
            rawSource = new IgniteBytecodeSource(facade);
            this.closeable = ignite;
            log.info("[RULES] loader=thin host={} port={} cacheName={}", igniteHost, ignitePort, cacheName);

        } else {
            throw new IllegalArgumentException("Unknown rules.loader=" + mode + " (use thin|http)");
        }

        this.bytecodeSource = new TimedBytecodeSource(rawSource, log::info);
        this.reloader = new RulesReloader(bytecodeSource, rulesRegistry);

        long t0 = System.nanoTime();
        reloader.reloadAllStrict(nameToLoad);
        long ms = (System.nanoTime() - t0) / 1_000_000;

        log.info("[RULES] initial reloadAllStrict('{}') finished in {}ms, loaded rules={}",
                nameToLoad, ms, rulesRegistry.snapshot().size());
    }

    @Override
    public void processElement(MqRecord in, ReadOnlyContext ctx, Collector<MqReply> out) {
        if (in == null || in.payload == null || in.payload.isBlank()) {
            log.warn("[PIPE][no-qid] Empty MQ payload");
            return;
        }

        String raw = in.payload;

        try {
            JsonNode originalEvent = mapper.readTree(raw);
            String qid = originalEvent.path("dfw_query_id").asText("no-qid");
            String dataset = originalEvent.path("dfw_dataset_code").asText("UNKNOWN_DATASET");

            ReadOnlyBroadcastState<String, Long> st = ctx.getBroadcastState(rulesBroadcastDesc);
            Long currentVersion = (st != null) ? st.get(KEY_VERSION) : null;

            log.info("[PIPE][{}] using rulesVersion={} rulesCount={}",
                    qid, currentVersion, rulesRegistry.snapshot().size());

            if (logPayloads) {
                log.info("[PIPE][{}] 1) MQ_IN:\n{}", qid, maskJsonPretty(raw));
            } else {
                log.info("[PIPE][{}] 1) MQ_IN preview={}", qid, preview(maskInline(raw), logPreviewLen));
            }

            FlatProfileDto profile = processor.toFlatProfile(originalEvent).orElse(null);
            if (profile == null) {
                log.warn("[PIPE][{}] Flat profile is null, skip.", qid);
                return;
            }

            Map<String, String> normalizedMap = profile.asStringMap();

            if (logPayloads) {
                log.info("[PIPE][{}] 2) NORMALIZED_MAP size={} keys={}", qid, normalizedMap.size(), normalizedMap.keySet());
                log.info("[PIPE][{}] 2) NORMALIZED_MAP values(masked)={}", qid, maskMap(normalizedMap));
            } else {
                log.info("[PIPE][{}] 2) NORMALIZED_MAP size={} keys={}", qid, normalizedMap.size(), normalizedMap.keySet());
            }

            Map<String, Rule> rules = rulesRegistry.snapshot();


                      // пока беру тестовые планы без фильтра !!!!!
            List<FieldRuleBinding> bindings = planRegistry.defaultPlan();
            if (bindings == null || bindings.isEmpty()) {
                log.warn("[PIPE][{}] No bindings plan found for dataset={}, skip", qid, dataset);
                return;
            }

            ValidationResult validation = validationService.validate(rules, normalizedMap, bindings);

            String shortJson = shortAnswerService.build(originalEvent, validation);
            if (shortJson == null) {
                log.warn("[PIPE][{}] ShortAnswerService returned null.", qid);
                return;
            }

            if (logPayloads) {
                log.info("[PIPE][{}] 3) ANSWER_SHORT:\n{}", qid, maskJsonPretty(shortJson));
            } else {
                log.info("[PIPE][{}] 3) ANSWER_SHORT preview={}", qid, preview(maskInline(shortJson), logPreviewLen));
            }

            String detailJson = detailAnswerService.build(originalEvent, validation);
            if (detailJson != null) {
                if (logPayloads) {
                    log.info("[PIPE][{}] 4) ANSWER_DETAIL:\n{}", qid, maskJsonPretty(detailJson));
                } else {
                    log.info("[PIPE][{}] 4) ANSWER_DETAIL preview={}", qid, preview(maskInline(detailJson), logPreviewLen));
                }
            } else {
                log.warn("[PIPE][{}] DetailAnswerService returned null.", qid);
            }

            out.collect(new MqReply(in.msgId, shortJson));

        } catch (Exception e) {
            log.error("Failed to build answers.", e);
        }
    }

    @Override
    public void processBroadcastElement(RulesVersionEvent ev, Context ctx, Collector<MqReply> out) throws Exception {
        if (ev == null || ev.version <= 0) return;

        BroadcastState<String, Long> st = ctx.getBroadcastState(rulesBroadcastDesc);
        Long current = st.get(KEY_VERSION);

        if (current != null && ev.version <= current) {
            log.info("[RULES][KAFKA] ignore version={} (current={})", ev.version, current);
            return;
        }

        log.info("[RULES][KAFKA] new version={} (prev={}) -> reloading rules...", ev.version, current);

        long t0 = System.nanoTime();
        try {
            reloader.reloadAllStrict(nameToLoad);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            int sz = rulesRegistry.snapshot().size();

            st.put(KEY_VERSION, ev.version);

            log.info("[RULES][KAFKA] reload OK version={} in {}ms, rules={}", ev.version, ms, sz);
        } catch (Exception ex) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.error("[RULES][KAFKA] reload FAILED version={} after {}ms (rules keep old snapshot)", ev.version, ms, ex);
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


    private String preview(String s, int max) {
        if (s == null) return "null";
        if (s.length() <= max) return s;
        return s.substring(0, max) + "...(+" + (s.length() - max) + " chars)";
    }

    private String maskInline(String s) {
        if (s == null) return null;
        return s
                .replaceAll("(\"birthdate\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"clientSnils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"snils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"number\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"series\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"departmentCode\"\\s*:\\s*\")[^\"]*(\")", "$1***$2");
    }

    private String maskJsonPretty(String json) {
        if (json == null || json.isBlank()) return json;
        try {
            JsonNode root = mapper.readTree(json);
            maskNode(root);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return maskInline(json);
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
        if (m == null) return Map.of();
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
