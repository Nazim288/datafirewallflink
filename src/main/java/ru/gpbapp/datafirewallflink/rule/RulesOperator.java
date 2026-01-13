package ru.gpbapp.datafirewallflink.rule;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteBytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.impl.IgniteClientFacadeImpl;

import java.util.Map;

public class RulesOperator extends RichMapFunction<String, String> {

    private transient CompiledRulesRegistry registry;
    private transient RulesReloader reloader;

    // держим, чтобы закрывать
    private transient BytecodeSource bytecodeSource;

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
    }

    @Override
    public String map(String value) {
        // пример: берём snapshot, чтобы работать с консистентной картой
        Map<String, Rule> rules = registry.snapshot();

        // TODO: применяй правила как нужно (например все, или конкретное по имени)
        // Rule r = rules.get("Rule1069");
        // if (r != null && r.apply(data)) { ... }

        return value;
    }

    @Override
    public void close() throws Exception {
        // закрываем Ignite и всё, что внутри
        if (bytecodeSource instanceof AutoCloseable c) {
            c.close();
        }
    }
}

