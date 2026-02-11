package ru.gpbapp.datafirewallflink.rule;

import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import com.gpb.datafirewall.model.Rule;

import java.util.*;

public final class RulesReloader {

    private final BytecodeSource source;
    private final CompiledRulesRegistry registry;

    public RulesReloader(BytecodeSource source, CompiledRulesRegistry registry) {
        this.source = Objects.requireNonNull(source, "source");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public void reloadAllStrict(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null/blank");
        }

        Map<String, byte[]> bytecodes = source.loadAll(name);
        if (bytecodes == null) {
            throw new IllegalStateException("BytecodeSource returned null for name=" + name);
        }

        List<String> classNames = new ArrayList<>(bytecodes.keySet());
        classNames.sort(String::compareTo);

        long totalBytes = 0;

        for (String clsName : classNames) {
            byte[] bytes = bytecodes.get(clsName);
            if (clsName == null || clsName.isBlank()) {
                throw new IllegalStateException("Found empty/null class name key in '" + name + "'");
            }
            if (bytes == null || bytes.length == 0) {
                throw new IllegalStateException("Empty bytecode for class '" + clsName + "' in '" + name + "'");
            }
            totalBytes += bytes.length;
        }

        RuleClassLoader cl = new RuleClassLoader(bytecodes);
        Map<String, Rule> newRules = new HashMap<>();

        for (String clsName : classNames) {
            try {
                Class<? extends Rule> ruleClass = cl.loadRule(clsName);
                Rule rule = ruleClass.getDeclaredConstructor().newInstance();
                newRules.put(clsName, rule);
            } catch (Throwable e) {
                throw new RuntimeException(
                        "Failed to load rule '" + clsName + "' from '" + name + "'. Available keys=" + classNames,
                        e
                );
            }
        }

        registry.replaceAll(newRules);

    }
}
