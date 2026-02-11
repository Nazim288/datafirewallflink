package ru.gpbapp.datafirewallflink.rule;

import com.gpb.datafirewall.model.Rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Потокобезопасное хранилище активного набора скомпилированных бизнес-правил.
 *
 * <p>Хранит текущий набор правил в виде неизменяемого snapshot'а,
 * обеспечивая неблокирующий и консистентный доступ для потоков обработки.</p>
 *
 * <p>Обновление набора правил выполняется атомарно через {@link #replaceAll(Map)},
 * что гарантирует, что поток обработки всегда работает либо со старой,
 * либо с новой версией правил, без промежуточных состояний.</p>
 *
 * <p>Используется в связке с {@link RulesReloader} для реализации безопасной
 * "горячей" перезагрузки правил.</p>
 */
public final class CompiledRulesRegistry {

    private final AtomicReference<Map<String, Rule>> rulesRef =
            new AtomicReference<>(Map.of());

    public Rule get(String name) {
        return rulesRef.get().get(name);
    }

               // immutable snapshot
    public Map<String, Rule> snapshot() {
        return rulesRef.get();
    }
              // atomic swap
    public void replaceAll(Map<String, Rule> newRules) {
        Objects.requireNonNull(newRules, "newRules must not be null");
        Map<String, Rule> copy = Collections.unmodifiableMap(new HashMap<>(newRules));
        rulesRef.set(copy);
    }

    public void clear() {
        rulesRef.set(Map.of());
    }
}

