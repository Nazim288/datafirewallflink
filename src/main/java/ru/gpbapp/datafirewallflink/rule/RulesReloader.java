package ru.gpbapp.datafirewallflink.rule;

import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Отвечает за полную и строгую перезагрузку правил из внешнего источника байткода.
 *
 * <p>Загружает скомпилированные классы правил из {@link BytecodeSource},
 * проверяет их корректность, загружает через собственный {@link RuleClassLoader},
 * создаёт экземпляры и атомарно обновляет {@link CompiledRulesRegistry}.</p>
 *
 * <p>Режим работы — <b>strict</b>: любая ошибка (битый байткод, неправильный класс,
 * ошибка загрузки или создания экземпляра) приводит к немедленному прерыванию
 * операции без изменения текущего набора правил.</p>
 *
 * <p>Обеспечивает детерминированный порядок загрузки классов для удобства диагностики.</p>
 */


public final class RulesReloader {
    private final BytecodeSource source;
    private final CompiledRulesRegistry registry;

    public RulesReloader(BytecodeSource source, CompiledRulesRegistry registry) {
        this.source = Objects.requireNonNull(source, "source");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /**
     * Strict:
     * - любой битый/пустой байткод -> ошибка
     * - любой класс, который не Rule -> ошибка
     * - любой failure при загрузке/инстанцировании -> ошибка
     */
    public void reloadAllStrict(String cacheName) {
        Objects.requireNonNull(cacheName, "cacheName");

        Map<String, byte[]> bytecodes = source.loadAll(cacheName);
        if (bytecodes == null) {
            throw new IllegalStateException("BytecodeSource returned null for cacheName=" + cacheName);
        }

        // Детеминированный порядок (удобно для диагностики)
        List<String> classNames = new ArrayList<>(bytecodes.keySet());
        classNames.sort(String::compareTo);

        // Strict-валидация входа
        for (String name : classNames) {
            byte[] bytes = bytecodes.get(name);
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("Found empty/null class name key in cache " + cacheName);
            }
            if (bytes == null || bytes.length == 0) {
                throw new IllegalStateException("Empty bytecode for class " + name + " in cache " + cacheName);
            }
        }

        RuleClassLoader cl = new RuleClassLoader(bytecodes);
        Map<String, Rule> newRules = new HashMap<>();

        for (String name : classNames) {
            try {
                Class<? extends Rule> ruleClass = cl.loadRule(name);
                Rule rule = ruleClass.getDeclaredConstructor().newInstance();
                newRules.put(name, rule);
            } catch (Throwable e) {
                // Throwable: ClassFormatError/LinkageError тоже сюда попадут
                throw new RuntimeException(
                        "Failed to load rule '" + name + "' from cache '" + cacheName + "'. " +
                                "Available keys=" + classNames, e);
            }
        }

        // Атомарная подмена
        registry.replaceAll(newRules);
    }
}
