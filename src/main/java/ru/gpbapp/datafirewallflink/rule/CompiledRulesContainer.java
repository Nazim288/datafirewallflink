package ru.gpbapp.datafirewallflink.rule;

import com.gpb.datafirewall.model.Rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Потокобезопасный контейнер для хранения текущего набора бизнес-правил.
 *
 * <p>Хранит правила в виде неизменяемого snapshot'а, доступного для чтения без блокировок,
 * и обеспечивает атомарную замену всего набора правил.</p>
 *
 * <p>Гарантирует, что все потоки обработки работают с консистентной версией правил
 * без промежуточных состояний.</p>
 *
 * <p>Используется как in-memory хранилище активных правил в механизме
 * динамической перезагрузки.</p>
 */
public final class CompiledRulesContainer {

    private final AtomicReference<Map<String, Rule>> rulesRef =
            new AtomicReference<>(Map.of()); // immutable empty map

    public Rule get(String name) {
        if (name == null) return null;
        return rulesRef.get().get(name);
    }

    /** Текущий снапшот (read-only). */
    public Map<String, Rule> snapshot() {
        return rulesRef.get();
    }

    /** Атомарная замена всего набора правил. */
    public void replaceAll(Map<String, Rule> newRules) {
        Objects.requireNonNull(newRules, "newRules");
        Map<String, Rule> copy = Collections.unmodifiableMap(new HashMap<>(newRules));
        rulesRef.set(copy);
    }

    public void clear() {
        rulesRef.set(Map.of());
    }

    public int size() {
        return rulesRef.get().size();
    }
}



