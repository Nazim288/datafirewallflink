package ru.gpbapp.datafirewallflink.rule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class RuleClassLoader extends ClassLoader {

    private final Map<String, byte[]> bytecodeByName;
    private final ConcurrentMap<String, Class<?>> defined = new ConcurrentHashMap<>();

    public RuleClassLoader(Map<String, byte[]> bytecodeByName) {
        super(RuleClassLoader.class.getClassLoader());
        this.bytecodeByName = Map.copyOf(bytecodeByName);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        // быстрый путь
        Class<?> cached = defined.get(name);
        if (cached != null) return cached;

        synchronized (this) {
            // повторная проверка внутри synchronized
            cached = defined.get(name);
            if (cached != null) return cached;

            byte[] bytes = bytecodeByName.get(name);
            if (bytes == null) {
                throw new ClassNotFoundException(name);
            }

            Class<?> cls = defineClass(name, bytes, 0, bytes.length);
            resolveClass(cls);               // важный шаг
            defined.put(name, cls);
            return cls;
        }
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Rule> loadRule(String name) throws ClassNotFoundException {
        Class<?> cls = loadClass(name, true);

        if (!Rule.class.isAssignableFrom(cls)) {
            throw new ClassCastException(
                    "Loaded class " + name + " does not implement Rule"
            );
        }

        return (Class<? extends Rule>) cls;
    }
}

