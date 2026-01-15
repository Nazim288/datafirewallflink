package ru.gpbapp.datafirewallflink.mq;

import java.util.Iterator;

/**
 * Упрощённый тестовый фасад для получения сообщений по одному,
 * как будто из MQ.
 */
public final class TestMqEmulator {

    private final EventSource source;
    private Iterator<String> iterator;

    public TestMqEmulator(EventSource source) {
        this.source = source;
    }

    public void start() {
        this.iterator = source.stream().iterator();
    }

    public String poll() {
        if (iterator == null) start();
        return iterator.hasNext() ? iterator.next() : null;
    }

    public void close() {
        try {
            source.close();
        } catch (Exception ignored) {}
    }
}

