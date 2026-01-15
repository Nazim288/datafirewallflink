package ru.gpbapp.datafirewallflink.mq;

import java.io.Closeable;
import java.util.stream.Stream;

/**
 * Абстракция источника входящих событий.
 * Реальные реализации: IBM MQ, Kafka, ...
 * Тестовая реализация: {@link MockMqEventSource}
 */
public interface EventSource extends Closeable {
    Stream<String> stream();
}

