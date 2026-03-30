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
import ru.gpbapp.datafirewallflink.cache.CompiledRulesRegistry;
import ru.gpbapp.datafirewallflink.cache.PoliticsControlAreaRulesCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsDatasetControlAreaCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsDatasetExclusionCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsErrorMessagesCache;
import ru.gpbapp.datafirewallflink.cache.PoliticsFilterFlagCache;
import ru.gpbapp.datafirewallflink.config.IgniteRulesApiClient;
import ru.gpbapp.datafirewallflink.converter.MappingNormalizer;
import ru.gpbapp.datafirewallflink.dto.CacheResponseDto;
import ru.gpbapp.datafirewallflink.dto.HttpBytecodeSource;
import ru.gpbapp.datafirewallflink.dto.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEvent;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.rule.RulesReloader;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class MqWithRulesReloadBroadcastProcessFunction
        extends BroadcastProcessFunction<MqRecord, CacheUpdateEvent, MqReply> {

    private static final Logger log =
            LoggerFactory.getLogger(MqWithRulesReloadBroadcastProcessFunction.class);

    private static final String CACHE_COMPILED_RULES = "compiled_rules";
    private static final String CACHE_POLITICS = "politics";

    private static final String CACHE_POLITICS_DATASET2CONTROL_AREA =
            "politics_dataset2control_area";
    private static final String CACHE_POLITICS_CONTROL_AREA_RULES =
            "politics_control_area_rules";
    private static final String CACHE_POLITICS_ERROR_MESSAGES =
            "politics_error_messages";
    private static final String CACHE_POLITICS_DATASET_EXCLUSION =
            "politics_dataset_exclusion";
    private static final String CACHE_POLITICS_FILTER_FLAG =
            "politics_filter_flag";

    private final MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc;

    private transient ObjectMapper mapper;

    private transient CompiledRulesRegistry rulesRegistry;
    private transient RulesReloader reloader;
    private transient BytecodeSource bytecodeSource;
    private transient AutoCloseable closeable;
    private transient IgniteRulesApiClient igniteApiClient;

    private transient PoliticsDatasetControlAreaCache politicsDataset2ControlAreaCache;
    private transient PoliticsControlAreaRulesCache politicsControlAreaRulesCache;
    private transient PoliticsErrorMessagesCache politicsErrorMessagesCache;
    private transient PoliticsDatasetExclusionCache politicsDatasetExclusionCache;
    private transient PoliticsFilterFlagCache politicsFilterFlagCache;

    private transient ValidationService validationService;
    private transient ShortAnswerService shortAnswerService;
    private transient DetailAnswerService detailAnswerService;
    private transient MappingNormalizer normalizer;

    private transient boolean logPayloads;
    private transient int logPreviewLen;

    /**
     * Конструктор функции.
     *
     * <p>Сохраняет descriptor broadcast-state, в котором будет лежать
     * последнее событие обновления кэша из Kafka.</p>
     */
    public MqWithRulesReloadBroadcastProcessFunction(
            MapStateDescriptor<String, CacheUpdateEvent> rulesBroadcastDesc
    ) {
        this.rulesBroadcastDesc = rulesBroadcastDesc;
    }

    /**
     * Инициализация функции при старте subtask.
     *
     * <p>Здесь создаются локальные runtime-кэши, нормализатор, сервисы ответов,
     * клиент Ignite API и инфраструктура загрузки compiled_rules.</p>
     *
     * <p>После инициализации выполняется bootstrap-загрузка актуальных версий
     * всех кэшей через Ignite latest API. Дальнейшие обновления приходят через Kafka.</p>
     */
    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();

        ParameterTool pt = (ParameterTool) rc.getExecutionConfig().getGlobalJobParameters();
        if (pt == null) {
            pt = ParameterTool.fromMap(Map.of());
        }

        this.logPayloads = pt.getBoolean("log.payloads", false);
        this.logPreviewLen = pt.getInt("log.preview.len", 600);

        this.mapper = new ObjectMapper();

        this.rulesRegistry = new CompiledRulesRegistry();
        this.politicsDataset2ControlAreaCache = new PoliticsDatasetControlAreaCache();
        this.politicsControlAreaRulesCache = new PoliticsControlAreaRulesCache();
        this.politicsErrorMessagesCache = new PoliticsErrorMessagesCache();
        this.politicsDatasetExclusionCache = new PoliticsDatasetExclusionCache();
        this.politicsFilterFlagCache = new PoliticsFilterFlagCache();

        initRulesLoaderAndLoad(pt);

        String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
        this.igniteApiClient = new IgniteRulesApiClient(igniteApiUrl);


        boolean bootstrapEnabled = pt.getBoolean("cache.bootstrap.enabled", true);
        boolean politicsBootstrapEnabled = pt.getBoolean("politics.bootstrap.enabled", false);        initTestCaches();
        if (bootstrapEnabled) {
            CacheBootstrapService bootstrapService = new CacheBootstrapService(
                    igniteApiClient,
                    reloader,
                    rulesRegistry,
                    politicsDataset2ControlAreaCache,
                    politicsControlAreaRulesCache,
                    politicsErrorMessagesCache,
                    politicsDatasetExclusionCache,
                    politicsFilterFlagCache,
                    politicsBootstrapEnabled
            );
            bootstrapService.initializeAll();
        } else {
            log.info("[INIT] startup cache bootstrap is disabled. Waiting for Kafka cache update events.");
        }

        this.validationService = new ValidationService();
        this.shortAnswerService = new ShortAnswerService(mapper);
        this.detailAnswerService = new DetailAnswerService(mapper);
        this.normalizer = new MappingNormalizer(mapper);

        log.info(
                "[INIT] subtask={} log.payloads={} rulesLoaded={} dataset2controlAreaLoaded={} controlAreaRulesLoaded={} errorMessagesLoaded={} datasetExclusionLoaded={} filterFlagLoaded={}",
                rc.getIndexOfThisSubtask(),
                logPayloads,
                rulesRegistry.size(),
                politicsDataset2ControlAreaCache.size(),
                politicsControlAreaRulesCache.size(),
                politicsErrorMessagesCache.size(),
                politicsDatasetExclusionCache.size(),
                politicsFilterFlagCache.size()
        );
    }

    /**
     * Настраивает источник загрузки compiled_rules.
     *
     * <p>Поддерживает два режима:</p>
     * <p>- http: читаем bytecode правил через Ignite REST API</p>
     * <p>- thin: читаем bytecode правил напрямую из Ignite thin client</p>
     *
     * <p>В конце создаётся {@link RulesReloader}, который потом либо во время bootstrap,
     * либо по Kafka-событию загрузит конкретную versioned-версию compiled_rules.</p>
     */
    private void initRulesLoaderAndLoad(ParameterTool pt) {
        String mode = pt.get("rules.loader", "http").toLowerCase(Locale.ROOT).trim();

        BytecodeSource rawSource;

        if ("http".equals(mode)) {
            String igniteApiUrl = pt.get("ignite.apiUrl", "http://127.0.0.1:8080");
            IgniteRulesApiClient apiClient = new IgniteRulesApiClient(igniteApiUrl);
            rawSource = new HttpBytecodeSource(apiClient);
            this.closeable = null;

            log.info("[RULES] loader=http apiUrl={}", igniteApiUrl);

        } else if ("thin".equals(mode)) {
            String igniteHost = pt.get("ignite.host", "127.0.0.1");
            int ignitePort = pt.getInt("ignite.port", 10800);

            IgniteClientFacadeImpl ignite = new IgniteClientFacadeImpl(igniteHost, ignitePort);
            IgniteClientFacade facade = ignite;
            rawSource = new IgniteBytecodeSource(facade);
            this.closeable = ignite;

            log.info("[RULES] loader=thin host={} port={}", igniteHost, ignitePort);

        } else {
            throw new IllegalArgumentException("Unknown rules.loader=" + mode + " (use thin|http)");
        }

        this.bytecodeSource = new TimedBytecodeSource(rawSource, log::info);
        this.reloader = new RulesReloader(bytecodeSource, rulesRegistry);

        log.info("[RULES] rules loader initialized. Actual cache versions will be loaded during startup bootstrap.");
    }

    /**
     * Обрабатывает входящее MQ-сообщение.
     *
     * <p>Основной runtime flow:</p>
     * <p>1. Парсит входной JSON</p>
     * <p>2. Берёт первый найденный dataset_code из event.data.*.dataset_code</p>
     * <p>3. По dataset_code находит controlArea в PoliticsDatasetControlAreaCache</p>
     * <p>4. По controlArea получает field->rules mapping из PoliticsControlAreaRulesCache</p>
     * <p>5. Нормализует входные данные в logicalField -> value</p>
     * <p>6. Валидирует данные с помощью compiled_rules и найденного набора правил</p>
     * <p>7. Строит short/detail ответ</p>
     */
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

            ReadOnlyBroadcastState<String, CacheUpdateEvent> st =
                    ctx.getBroadcastState(rulesBroadcastDesc);

            CacheUpdateEvent compiledRulesEvent =
                    st != null ? st.get(CACHE_COMPILED_RULES) : null;
            CacheUpdateEvent politicsEvent =
                    st != null ? st.get(CACHE_POLITICS) : null;

            Long compiledRulesVersion = compiledRulesEvent != null
                    ? compiledRulesEvent.version
                    : null;
            Long politicsVersion = politicsEvent != null
                    ? politicsEvent.version
                    : null;

            log.info(
                    "[PIPE][{}] using compiledRulesVersion={} politicsVersion={} rulesCount={}",
                    qid,
                    compiledRulesVersion,
                    politicsVersion,
                    rulesRegistry.size()
            );

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 1) MQ_IN:\n{}", qid, maskJsonPretty(raw));
            }

            String datasetCode = extractFirstDatasetCode(originalEvent);
            if (datasetCode == null || datasetCode.isBlank()) {
                log.warn("[PIPE][{}] dataset_code not found in input payload", qid);
                return;
            }

            String controlArea = politicsDataset2ControlAreaCache.get(datasetCode);
            if (controlArea == null || controlArea.isBlank()) {
                log.warn("[PIPE][{}] controlArea not found for datasetCode={}", qid, datasetCode);
                return;
            }

            Map<String, Set<String>> fieldToRules = politicsControlAreaRulesCache.get(controlArea);
            if (fieldToRules == null || fieldToRules.isEmpty()) {
                log.warn("[PIPE][{}] fieldToRules not found for controlArea={} datasetCode={}",
                        qid, controlArea, datasetCode);
                return;
            }

            log.info("[PIPE][{}] datasetCode={} controlArea={} mappedFields={}",
                    qid, datasetCode, controlArea, fieldToRules.keySet());

            Map<String, String> normalizedMap = normalizer.normalize(originalEvent);

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 2) NORMALIZED_MAP size={} keys={}",
                        qid, normalizedMap.size(), normalizedMap.keySet());
                log.info("[PIPE][{}] 2) NORMALIZED_MAP full(masked):\n{}",
                        qid,
                        prettyObject(maskMap(normalizedMap)));
            }

            Map<String, Rule> compiledRules = rulesRegistry.snapshot();

            ValidationResult validation = validationService.validate(
                    compiledRules,
                    normalizedMap,
                    fieldToRules
            );

            String shortJson = shortAnswerService.build(originalEvent, validation);
            if (shortJson == null) {
                log.warn("[PIPE][{}] ShortAnswerService returned null.", qid);
                return;
            }

            if (logPayloads && log.isInfoEnabled()) {
                log.info("[PIPE][{}] 3) ANSWER_SHORT:\n{}", qid, maskJsonPretty(shortJson));
            }

            String detailJson = detailAnswerService.build(originalEvent, validation);
            if (detailJson != null) {
                if (logPayloads && log.isInfoEnabled()) {
                    log.info("[PIPE][{}] 4) ANSWER_DETAIL:\n{}", qid, maskJsonPretty(detailJson));
                }
            } else {
                log.warn("[PIPE][{}] DetailAnswerService returned null.", qid);
            }

            out.collect(new MqReply(in.msgId, shortJson));

        } catch (Exception e) {
            log.error("Failed to build answers.", e);
        }
    }

    /**
     * Обрабатывает событие обновления кэша из Kafka.
     *
     * <p>Начальная инициализация кэшей выполняется при старте через Ignite latest API.</p>
     * <p>После старта новые версии compiled_rules и politics bundle приезжают через Kafka.</p>
     */
    @Override
    public void processBroadcastElement(
            CacheUpdateEvent ev,
            Context ctx,
            Collector<MqReply> out
    ) throws Exception {
        if (ev == null || !ev.isValid()) {
            return;
        }

        BroadcastState<String, CacheUpdateEvent> st =
                ctx.getBroadcastState(rulesBroadcastDesc);

        CacheUpdateEvent current = st.get(ev.cacheName);

        if (current != null && ev.version <= current.version) {
            log.info("[CACHE][KAFKA] ignore cacheName={} version={} (current={})",
                    ev.cacheName, ev.version, current.version);
            return;
        }

        log.info("[CACHE][KAFKA] new event cacheName={} version={} (prev={}) -> reloading...",
                ev.cacheName,
                ev.version,
                current != null ? current.version : null);

        long t0 = System.nanoTime();
        try {
            switch (ev.cacheName) {
                case CACHE_COMPILED_RULES -> reloadCompiledRules(ev.version);
                case CACHE_POLITICS -> reloadPoliticsCaches(ev.version);
                default -> {
                    log.warn("[CACHE][KAFKA] unsupported cacheName={}", ev.cacheName);
                    return;
                }
            }

            long ms = (System.nanoTime() - t0) / 1_000_000;
            st.put(ev.cacheName, ev);

            log.info("[CACHE][KAFKA] reload OK cacheName={} version={} in {}ms",
                    ev.cacheName, ev.version, ms);

        } catch (Exception ex) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.error("[CACHE][KAFKA] reload FAILED cacheName={} version={} after {}ms (keep old snapshot)",
                    ev.cacheName, ev.version, ms, ex);
        }
    }

    /**
     * Загружает новую версию compiled_rules.
     *
     * <p>Из version формирует имя Ignite-cache вида compiled_rules_${version},
     * затем через RulesReloader собирает новые Rule-объекты и атомарно публикует их
     * в CompiledRulesRegistry.</p>
     */
    private void reloadCompiledRules(long version) {
        String fullCacheName = CACHE_COMPILED_RULES + "_" + version;

        log.info("[CACHE] Reloading compiled rules from {}", fullCacheName);

        reloader.reloadAllStrict(fullCacheName);

        log.info("[CACHE] compiled_rules reloaded: version={}, size={}",
                version, rulesRegistry.size());
    }

    /**
     * Загружает новую версию politics bundle.
     *
     * <p>Читает из Ignite API versioned-кэши:</p>
     * <p>- politics_dataset2control_area_${version}</p>
     * <p>- politics_control_area_rules_${version}</p>
     * <p>- politics_error_messages_${version}</p>
     * <p>- politics_dataset_exclusion_${version}</p>
     * <p>- politics_filter_flag_${version}</p>
     *
     * <p>После чтения конвертирует payload в нужные структуры и атомарно публикует их
     * в локальные snapshot-кэши.</p>
     */
    private void reloadPoliticsCaches(long version) {
        String versionStr = String.valueOf(version);

        String dataset2ControlAreaName = CACHE_POLITICS_DATASET2CONTROL_AREA + "_" + version;
        String controlAreaRulesName = CACHE_POLITICS_CONTROL_AREA_RULES + "_" + version;
        String errorMessagesName = CACHE_POLITICS_ERROR_MESSAGES + "_" + version;
        String datasetExclusionName = CACHE_POLITICS_DATASET_EXCLUSION + "_" + version;
        String filterFlagName = CACHE_POLITICS_FILTER_FLAG + "_" + version;

        log.info("[CACHE] Reloading politics bundle for version={}", version);

        CacheResponseDto<String, Object> dataset2ControlAreaResponse =
                igniteApiClient.getVersionedCache(dataset2ControlAreaName);

        CacheResponseDto<String, Object> controlAreaRulesResponse =
                igniteApiClient.getVersionedCache(controlAreaRulesName);

        CacheResponseDto<String, Object> errorMessagesResponse =
                igniteApiClient.getVersionedCache(errorMessagesName);

        CacheResponseDto<String, Object> datasetExclusionResponse =
                igniteApiClient.getVersionedCache(datasetExclusionName);

        CacheResponseDto<String, Object> filterFlagResponse =
                igniteApiClient.getVersionedCache(filterFlagName);

        Map<String, String> dataset2ControlArea =
                toStringMap(dataset2ControlAreaResponse.getCache(), dataset2ControlAreaName);

        Map<String, Map<String, Set<String>>> controlAreaRules =
                toNestedRulesMap(controlAreaRulesResponse.getCache(), controlAreaRulesName);

        Map<String, String> errorMessages =
                toStringMap(errorMessagesResponse.getCache(), errorMessagesName);

        Map<String, Set<String>> datasetExclusion =
                toSetMap(datasetExclusionResponse.getCache(), datasetExclusionName);

        Map<String, Boolean> filterFlags =
                toBooleanMap(filterFlagResponse.getCache(), filterFlagName);

        politicsDataset2ControlAreaCache.replaceAll(dataset2ControlArea, versionStr);
        politicsControlAreaRulesCache.replaceAll(controlAreaRules, versionStr);
        politicsErrorMessagesCache.replaceAll(errorMessages, versionStr);
        politicsDatasetExclusionCache.replaceAll(datasetExclusion, versionStr);
        politicsFilterFlagCache.replaceAll(filterFlags, versionStr);

        log.info(
                "[CACHE] politics reloaded: version={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                version,
                dataset2ControlArea.size(),
                controlAreaRules.size(),
                errorMessages.size(),
                datasetExclusion.size(),
                filterFlags.size()
        );
    }

    /**
     * Из входного JSON ищет первый dataset_code внутри event.data.
     *
     * <p>Например из event.data.homeAddress.dataset_code вернёт
     * "УС.ЛиК.Адрес проживания".</p>
     *
     * <p>Этот dataset_code потом используется как ключ в PoliticsDatasetControlAreaCache.</p>
     */
    private String extractFirstDatasetCode(JsonNode originalEvent) {
        JsonNode dataNode = originalEvent.path("data");
        if (!dataNode.isObject()) {
            return null;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            JsonNode child = entry.getValue();
            if (child != null && child.isObject()) {
                JsonNode datasetCodeNode = child.get("dataset_code");
                if (datasetCodeNode != null && !datasetCodeNode.isNull()) {
                    String datasetCode = datasetCodeNode.asText(null);
                    if (datasetCode != null && !datasetCode.isBlank()) {
                        return datasetCode.trim();
                    }
                }
            }
        }

        return null;
    }

    /**
     * Конвертирует payload из Ignite API в Map<String, String>.
     *
     * <p>Используется для кэшей, где структура простая:
     * key -> string value.</p>
     */
    private Map<String, String> toStringMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }
            if (entry.getValue() == null) {
                throw new IllegalStateException(
                        "Null value for key '" + entry.getKey() + "' in cache " + cacheName
                );
            }

            result.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return result;
    }

    /**
     * Конвертирует payload из Ignite API в Map<String, Boolean>.
     *
     * <p>Используется для politics_filter_flag.</p>
     */
    private Map<String, Boolean> toBooleanMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, Boolean> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }
            if (entry.getValue() == null) {
                throw new IllegalStateException(
                        "Null value for key '" + entry.getKey() + "' in cache " + cacheName
                );
            }

            Boolean value = mapper.convertValue(entry.getValue(), Boolean.class);
            result.put(entry.getKey(), value);
        }
        return result;
    }

    /**
     * Конвертирует payload из Ignite API в Map<String, Set<String>>.
     *
     * <p>Используется для структур вида:
     * key -> список/множество строк.</p>
     */
    private Map<String, Set<String>> toSetMap(Map<String, Object> payload, String cacheName) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }

            Set<String> value = mapper.convertValue(
                    entry.getValue(),
                    mapper.getTypeFactory().constructCollectionType(Set.class, String.class)
            );

            result.put(entry.getKey(), value);
        }
        return result;
    }

    /**
     * Конвертирует payload из Ignite API в Map<String, Map<String, Set<String>>>.
     *
     * <p>Используется для politics_control_area_rules, где:
     * controlArea -> (logicalField -> set(ruleName)).</p>
     */
    private Map<String, Map<String, Set<String>>> toNestedRulesMap(
            Map<String, Object> payload,
            String cacheName
    ) {
        if (payload == null) {
            throw new IllegalStateException("Payload is null for cache " + cacheName);
        }

        var typeFactory = mapper.getTypeFactory();
        var setType = typeFactory.constructCollectionType(Set.class, String.class);
        var innerMapType = typeFactory.constructMapType(
                LinkedHashMap.class,
                typeFactory.constructType(String.class),
                setType
        );

        Map<String, Map<String, Set<String>>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalStateException("Null key in cache " + cacheName);
            }

            Map<String, Set<String>> value = mapper.convertValue(
                    entry.getValue(),
                    innerMapType
            );

            result.put(entry.getKey(), value);
        }
        return result;
    }

    /**
     * Освобождает внешние ресурсы при остановке функции.
     *
     * <p>Нужно главным образом для thin-client подключения к Ignite.</p>
     */
    @Override
    public void close() {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close resources", e);
        }
    }

    /**
     * Преобразует объект в pretty JSON/string для логирования.
     */
    private String prettyObject(Object o) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (Exception e) {
            return String.valueOf(o);
        }
    }

    /**
     * Делает простую inline-маскировку чувствительных полей в строковом JSON.
     *
     * <p>Используется как fallback, если красивый JSON-парсинг не удался.</p>
     */
    private String maskInline(String s) {
        if (s == null) {
            return null;
        }
        return s
                .replaceAll("(\"birthdate\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"clientSnils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"snils\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"number\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"series\"\\s*:\\s*\")[^\"]*(\")", "$1***$2")
                .replaceAll("(\"departmentCode\"\\s*:\\s*\")[^\"]*(\")", "$1***$2");
    }

    /**
     * Формирует pretty JSON и маскирует чувствительные поля перед логированием.
     */
    private String maskJsonPretty(String json) {
        if (json == null || json.isBlank()) {
            return json;
        }
        try {
            JsonNode root = mapper.readTree(json);
            maskNode(root);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return maskInline(json);
        }
    }

    /**
     * Рекурсивно проходит по JsonNode и затирает чувствительные поля.
     */
    private void maskNode(JsonNode node) {
        if (node == null) {
            return;
        }
        if (node.isObject()) {
            Iterator<String> it = node.fieldNames();
            while (it.hasNext()) {
                String fn = it.next();
                JsonNode child = node.get(fn);
                if (isSensitiveKey(fn)
                        && node instanceof com.fasterxml.jackson.databind.node.ObjectNode obj) {
                    obj.put(fn, "***");
                } else {
                    maskNode(child);
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                maskNode(child);
            }
        }
    }

    /**
     * Определяет, относится ли ключ к чувствительным данным,
     * которые нельзя писать в лог в открытом виде.
     */
    private boolean isSensitiveKey(String key) {
        if (key == null) {
            return false;
        }
        String k = key.toLowerCase(Locale.ROOT);
        return k.equals("birthdate")
                || k.equals("clientsnils")
                || k.equals("snils")
                || k.equals("inn")
                || k.equals("number")
                || k.equals("series")
                || k.equals("departmentcode");
    }

    /**
     * Маскирует чувствительные значения в нормализованной map перед логированием.
     */
    private Map<String, String> maskMap(Map<String, String> m) {
        if (m == null) {
            return Map.of();
        }

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

    private void initTestCaches() {
        rulesRegistry.replaceAll(Map.of(), "test");

        politicsDataset2ControlAreaCache.replaceAll(
                Map.of(
                        "УС.ЛиК.Адрес проживания", "Дашборд.УС ЛИК",
                        "УС.ЛиК.Адрес регистрации", "Дашборд.УС ЛИК",
                        "УС.ЛИК.Контакты клиента", "Дашборд.УС ЛИК",
                        "УС.ЛиК.Данные клиента", "Дашборд.УС ЛИК",
                        "УС.ЛиК.Документы клиента", "Дашборд.УС ЛИК"
                ),
                "test"
        );

        politicsControlAreaRulesCache.replaceAll(
                Map.of(
                        "Дашборд.УС ЛИК", Map.ofEntries(
                                Map.entry("АДРЕС.Страна", Set.of("Rule1164", "Rule1169")),
                                Map.entry("АДРЕС.Район", Set.of("Rule1194")),
                                Map.entry("АДРЕС.Код страны", Set.of("Rule1193", "Rule1192")),
                                Map.entry("АДРЕС.Населенный пункт", Set.of("Rule1166", "Rule10198", "Rule10196")),
                                Map.entry("АДРЕС.Улица", Set.of("Rule10213", "Rule10189", "Rule10188")),
                                Map.entry("АДРЕС.Почтовый индекс", Set.of("Rule1099", "Rule1098")),
                                Map.entry("АДРЕС.Наименование города", Set.of("Rule1094", "Rule1096", "Rule1097", "Rule1095", "Rule1144")),

                                Map.entry("КОНТАКТ.Телефон.Номер телефона", Set.of("Rule1146", "Rule1178", "Rule1196")),
                                Map.entry("КОНТАКТ.Почта.Электронный адрес (email)", Set.of("Rule1174")),

                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.СНИЛС", Set.of("Rule1180", "Rule1135", "Rule1182", "Rule1080", "Rule1181")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Имя", Set.of("Rule1070", "Rule1068", "Rule1066", "Rule1069", "Rule10192")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Фамилия", Set.of("Rule10328", "Rule1081", "Rule1083", "Rule10121", "Rule1084")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Отчество", Set.of("Rule1162", "Rule1078", "Rule1076", "Rule10194", "Rule10067")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Дата рождения", Set.of("Rule1158", "Rule10185", "Rule1148", "Rule10127")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Место рождения", Set.of("Rule1161", "Rule1075", "Rule1073", "Rule1072", "Rule1074")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Пол", Set.of("Rule1079")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.ФИО одной строкой", Set.of("Rule1091", "Rule1087", "Rule1090", "Rule1088", "Rule1175")),
                                Map.entry("ОСНОВНЫЕ СВЕДЕНИЯ.Дата смерти", Set.of("Rule1065")),

                                Map.entry("ИНН.Номер свидетельства", Set.of("Rule1092", "Rule1093", "Rule1086")),

                                Map.entry("ДУЛ.Паспорт РФ.Номер", Set.of("Rule1107", "Rule10184")),
                                Map.entry("ДУЛ.Паспорт РФ.Серия", Set.of("Rule10220", "Rule1109", "Rule1112")),
                                Map.entry("ДУЛ.Паспорт РФ.Кем выдан", Set.of("Rule1108", "Rule1165", "Rule10201", "Rule1110", "Rule1179")),
                                Map.entry("ДУЛ.Паспорт РФ.Дата выдачи", Set.of("Rule10133", "Rule10244", "Rule10132", "Rule10135", "Rule10134", "Rule1103")),
                                Map.entry("ДУЛ.Паспорт РФ.Код подразделения", Set.of("Rule1106", "Rule1105", "Rule1104")),

                                Map.entry("ГРАЖДАНСТВО.Страна", Set.of("Rule10148", "Rule1085"))
                        )
                ),
                "test"
        );

        politicsErrorMessagesCache.replaceAll(
                Map.of(
                        "Rule1164", "Тестовое сообщение для Rule1164",
                        "Rule1174", "Тестовое сообщение для Rule1174",
                        "Rule1158", "Тестовое сообщение для Rule1158"
                ),
                "test"
        );

        politicsDatasetExclusionCache.replaceAll(
                Map.of(
                        "УС.ЛиК.Адрес проживания", Set.of(),
                        "УС.ЛиК.Адрес регистрации", Set.of()
                ),
                "test"
        );

        politicsFilterFlagCache.replaceAll(
                Map.of(
                        "УС.ЛиК.Адрес проживания", Boolean.TRUE,
                        "УС.ЛиК.Адрес регистрации", Boolean.TRUE,
                        "УС.ЛИК.Контакты клиента", Boolean.TRUE,
                        "УС.ЛиК.Данные клиента", Boolean.TRUE,
                        "УС.ЛиК.Документы клиента", Boolean.TRUE
                ),
                "test"
        );

        log.info(
                "[TEST] caches initialized manually: rules={}, dataset2controlArea={}, controlAreaRules={}, errorMessages={}, datasetExclusion={}, filterFlag={}",
                rulesRegistry.size(),
                politicsDataset2ControlAreaCache.size(),
                politicsControlAreaRulesCache.size(),
                politicsErrorMessagesCache.size(),
                politicsDatasetExclusionCache.size(),
                politicsFilterFlagCache.size()
        );
    }
}