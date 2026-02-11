package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.IgniteRulesApiClient;
import ru.gpbapp.datafirewallflink.dto.FlatProfileDto;
import ru.gpbapp.datafirewallflink.dto.HttpBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.rule.CompiledRulesRegistry;
import com.gpb.datafirewall.model.Rule;
import ru.gpbapp.datafirewallflink.rule.RulesReloader;
import ru.gpbapp.datafirewallflink.validation.DetailsTemplateDynamic;
import ru.gpbapp.datafirewallflink.validation.FieldRuleBinding;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ShortAnswerMapFunction extends RichMapFunction<MqRecord, MqReply> {

    private static final Logger log = LoggerFactory.getLogger(ShortAnswerMapFunction.class);

    private transient ObjectMapper mapper;
    private transient JsonEventProcessor processor;

    private transient CompiledRulesRegistry registry;
    private transient RulesReloader reloader;
    private transient BytecodeSource bytecodeSource;

    private transient AutoCloseable closeable;

    private transient DetailsTemplateDynamic detailsTemplate;
    private transient ValidationService validationService;
    private transient ShortAnswerService shortAnswerService;

    private transient List<FieldRuleBinding> bindings;

    @Override
    public void open(Configuration parameters) {
        this.registry = new CompiledRulesRegistry();

        ParameterTool pt = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();

        String mode = pt.get("rules.loader", "http").toLowerCase(Locale.ROOT).trim();

        String sourceName = pt.get("rules.sourceName", "my-source");
        String cacheName = pt.get("ignite.cache", "compiled_" + sourceName);
        String nameToLoad = "http".equals(mode) ? sourceName : cacheName;

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

        this.bytecodeSource = new TimedBytecodeSource(rawSource, msg -> log.info(msg));
        this.reloader = new RulesReloader(bytecodeSource, registry);

        long t0 = System.nanoTime();
        reloader.reloadAllStrict(nameToLoad);
        long ms = (System.nanoTime() - t0) / 1_000_000;

        log.info("[RULES] reloadAllStrict('{}') finished in {}ms, loaded rules={}",
                nameToLoad, ms, registry.snapshot().size());

        log.info("[RULES] loaded keys={}", registry.snapshot().keySet());

        this.mapper = new ObjectMapper();
        this.processor = new JsonEventProcessor(mapper);

        this.detailsTemplate = new DetailsTemplateDynamic(mapper);
        this.validationService = new ValidationService(detailsTemplate);
        this.shortAnswerService = new ShortAnswerService(mapper);

        this.bindings = List.of(
                new FieldRuleBinding(
                        "ru.gpbapp.datafirewallflink.rules.RuleNameCheck",
                        "baseInfo",
                        "name",
                        "ОСНОВНЫЕ СВЕДЕНИЯ.Имя"
                ),
                new FieldRuleBinding(
                        "ru.gpbapp.datafirewallflink.rules.RuleBirthdateCheck",
                        "baseInfo",
                        "birthdate",
                        "ОСНОВНЫЕ СВЕДЕНИЯ-Дата рождения"
                ),
                new FieldRuleBinding(
                        "ru.gpbapp.datafirewallflink.rules.RuleSnilsCheck",
                        "documents",
                        "clientSnils",
                        "ОСНОВНЫЕ СВЕДЕНИЯ, СНИЛС"
                ),
                new FieldRuleBinding(
                        "ru.gpbapp.datafirewallflink.rules.RulePassportNumberCheck",
                        "clientIdCard0",
                        "number",
                        "ДУЛ.Паспорт РФ.Номер"
                )
        );
    }

    @Override
    public MqReply map(MqRecord in) {
        if (in == null || in.payload == null || in.payload.isBlank()) {
            log.warn("Empty MQ payload");
            return null;
        }

        try {
            JsonNode originalEvent = mapper.readTree(in.payload);

            FlatProfileDto profile = processor.toFlatProfile(originalEvent).orElse(null);
            if (profile == null) {
                log.warn("Flat profile is null, skip. dfw_query_id={}",
                        originalEvent.path("dfw_query_id").asText(null));
                return null;
            }

            Map<String, Rule> rules = registry.snapshot();

            ValidationResult validation = validationService.validate(
                    rules,
                    profile.asStringMap(),
                    bindings
            );

            String shortJson = shortAnswerService.build(originalEvent, validation);
            if (shortJson == null) {
                log.warn("ShortAnswerService returned null. dfw_query_id={}",
                        originalEvent.path("dfw_query_id").asText(null));
                return null;
            }

            return new MqReply(in.msgId, shortJson);

        } catch (Exception e) {
            String s = in.payload;
            String preview = s.length() > 200 ? s.substring(0, 200) : s;
            log.error("Failed to build short answer. Payload(first 200)={}", preview, e);
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
}
