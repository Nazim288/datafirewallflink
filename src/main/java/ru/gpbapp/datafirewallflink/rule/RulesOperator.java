package ru.gpbapp.datafirewallflink.rule;

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
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.services.DetailAnswerService;
import ru.gpbapp.datafirewallflink.services.ShortAnswerService;
import ru.gpbapp.datafirewallflink.services.TimedBytecodeSource;
import ru.gpbapp.datafirewallflink.services.ValidationService;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.Locale;
import java.util.Map;

public class RulesOperator extends RichMapFunction<String, String> {

    private static final Logger log = LoggerFactory.getLogger(RulesOperator.class);

    private transient CompiledRulesRegistry registry;
    private transient RulesReloader reloader;
    private transient BytecodeSource bytecodeSource;

    private transient AutoCloseable closeable;

    private transient ObjectMapper mapper;

    private transient MappingNormalizer normalizer;
    private transient ValidationService validationService;
    private transient ShortAnswerService shortAnswerService;
    private transient DetailAnswerService detailAnswerService;

    @Override
    public void open(Configuration parameters) {
        this.registry = new CompiledRulesRegistry();

        ParameterTool pt = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();

        String mode = pt.get("rules.loader", "thin").toLowerCase(Locale.ROOT).trim();

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

        this.mapper = new ObjectMapper();

        this.normalizer = new MappingNormalizer(mapper);
        this.validationService = new ValidationService();
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);

        log.info("[INIT] subtask={} rulesLoaded={} rulePlanFields={}",
                getRuntimeContext().getIndexOfThisSubtask(),
                registry.snapshot().size(),
                RulePlan.FIELD_TO_RULES.size());
    }

    @Override
    public String map(String value) {
        if (value == null || value.isBlank()) return null;

        try {
            JsonNode original = mapper.readTree(value);
            String qid = original.path("dfw_query_id").asText("no-qid");
            String dataset = original.path("dfw_dataset_code").asText("UNKNOWN_DATASET");

            Map<String, String> normalizedMap = normalizer.normalize(original);

            log.info("[PIPE][{}] dataset={} normalizedMap.size={} rulePlanFields={}",
                    qid, dataset, normalizedMap.size(), RulePlan.FIELD_TO_RULES.size());

            Map<String, Rule> rules = registry.snapshot();

            ValidationResult validation = validationService.validate(
                    rules,
                    normalizedMap,
                    RulePlan.FIELD_TO_RULES
            );

            String shortJson = shortAnswerService.build(original, validation);


            return shortJson;

        } catch (Exception e) {
            log.warn("RulesOperator failed to process event (first 200 chars): {}",
                    value.length() > 200 ? value.substring(0, 200) + "..." : value, e);
            return null;
        }
    }

    @Override
    public void close() {
        try {
            if (closeable != null) closeable.close();
        } catch (Exception e) {
            log.warn("Failed to close rules resources", e);
        }
    }
}