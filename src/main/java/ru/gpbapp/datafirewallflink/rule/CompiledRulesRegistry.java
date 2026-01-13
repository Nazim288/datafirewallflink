package ru.gpbapp.datafirewallflink.rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class CompiledRulesRegistry {

    private final AtomicReference<Map<String, Rule>> rulesRef =
            new AtomicReference<>(Map.of()); // immutable empty snapshot

    public Rule get(String name) {
        return rulesRef.get().get(name);
    }

    public Map<String, Rule> snapshot() {
        return rulesRef.get(); // immutable snapshot
    }

    public void replaceAll(Map<String, Rule> newRules) {
        Objects.requireNonNull(newRules, "newRules must not be null");
        Map<String, Rule> copy = Collections.unmodifiableMap(new HashMap<>(newRules));
        rulesRef.set(copy); // atomic swap
    }

    public void clear() {
        rulesRef.set(Map.of());
    }
}

