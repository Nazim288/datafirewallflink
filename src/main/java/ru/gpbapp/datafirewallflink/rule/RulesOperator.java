package ru.gpbapp.datafirewallflink.rule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import ru.gpbapp.datafirewallflink.dto.FlatProfileDto;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;
import ru.gpbapp.datafirewallflink.services.JsonEventProcessor;
import ru.gpbapp.datafirewallflink.services.ValidationService;
import ru.gpbapp.datafirewallflink.validation.AnswerBuilder;
import ru.gpbapp.datafirewallflink.validation.DetailsTemplateDynamic;
import ru.gpbapp.datafirewallflink.validation.FieldRuleBinding;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

import java.util.List;
import java.util.Map;

/**
 * Flink-оператор, который загружает и применяет динамические бизнес-правила во время обработки потока.
 *
 * <p>При инициализации:</p>
 * <ol>
 *     <li>читает параметры подключения из глобальных параметров Flink Job</li>
 *     <li>подключается к Ignite и получает доступ к кэшу с байткодом правил</li>
 *     <li>загружает и компилирует правила через {@link RulesReloader}</li>
 *     <li>сохраняет активный набор правил в {@link CompiledRulesRegistry}</li>
 * </ol>
 *
 * <p>Во время обработки сообщений оператор:</p>
 * <ol>
 *     <li>преобразует входной event JSON в плоский профиль (flat)</li>
 *     <li>берёт snapshot правил из registry</li>
 *     <li>прогоняет профиль через правила</li>
 *     <li>формирует ANSWER JSON (details + dfw_* + PROCESS_STATUS)</li>
 * </ol>
 *
 * <p>Поддерживает корректное освобождение ресурсов (закрытие Ignite) при завершении задачи.</p>
 */
public class RulesOperator extends RichMapFunction<String, String> {

    private transient CompiledRulesRegistry registry;
    private transient RulesReloader reloader;

    // держим, чтобы закрывать
    private transient BytecodeSource bytecodeSource;

    // JSON + flat converter
    private transient ObjectMapper mapper;
    private transient JsonEventProcessor processor;

    // validation/answer
    private transient DetailsTemplateDynamic detailsTemplate;
    private transient ValidationService validationService;
    private transient AnswerBuilder answerBuilder;

    // mapping: какое правило -> куда писать статус + logical key для detail
    private transient List<FieldRuleBinding> bindings;

    @Override
    public void open(Configuration parameters) {
        this.registry = new CompiledRulesRegistry();

        // 1) читаем параметры, которые прокинуты в env.getConfig().setGlobalJobParameters(...)
        ParameterTool pt = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();

        // значения по умолчанию — чтобы локально запускалось
        String igniteHost = pt.get("ignite.host", "127.0.0.1");
        int ignitePort = pt.getInt("ignite.port", 10800);
        String cacheName = pt.get("ignite.cache", "my-source");

        // 2) создаём источник байткода (thin client внутри)
        IgniteClientFacade ignite = new IgniteClientFacadeImpl(igniteHost, ignitePort);
        this.bytecodeSource = new IgniteBytecodeSource(ignite);

        // 3) reloader, который загрузит классы и положит в registry
        this.reloader = new RulesReloader(bytecodeSource, registry);

        // начальная загрузка
        reloader.reloadAllStrict(cacheName);

        // 4) JSON/flat processor
        this.mapper = new ObjectMapper();
        this.processor = new JsonEventProcessor(mapper);

        // 5) validation/answer builders
        this.detailsTemplate = new DetailsTemplateDynamic(mapper);
        this.validationService = new ValidationService(detailsTemplate);
        this.answerBuilder = new AnswerBuilder(mapper);

        // 6) биндинги: ruleName должен совпадать с ключами в registry.snapshot().keySet()
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
    public String map(String value) {
        try {
            // 1) parse original event (для dfw_* в ответе)
            JsonNode original = mapper.readTree(value);

            // 2) flat profile (DTO)
            FlatProfileDto profile = processor.toFlatProfile(value).orElse(null);
            if (profile == null) return null;

            // 3) rules snapshot
            Map<String, Rule> rules = registry.snapshot();

            // 4) validate (details строится внутри ValidationService через DetailsTemplateDynamic)
            ValidationResult validation = validationService.validate(
                    rules,
                    profile.asStringMap(),
                    bindings
            );

            // 5) build ANSWER json
            ObjectNode answer = answerBuilder.buildAnswer(original, validation);

            return mapper.writeValueAsString(answer);

        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        // закрываем Ignite и всё, что внутри
        if (bytecodeSource instanceof AutoCloseable c) {
            c.close();
        }
    }
}
