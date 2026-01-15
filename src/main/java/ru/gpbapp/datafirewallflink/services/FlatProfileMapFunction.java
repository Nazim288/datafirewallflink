package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Flink-функция преобразования входящих JSON-сообщений в плоский профиль.
 *
 * <p>На каждом узле выполнения инициализирует {@link JsonEventProcessor} и для
 * каждого входящего JSON-сообщения пытается преобразовать его в нормализованный
 * (flat) JSON-профиль.</p>
 *
 * <p>Некорректные, пустые и ошибочные сообщения безопасно отбрасываются
 * (возвращается {@code null}), что предотвращает сбои в потоковой обработке.</p>
 *
 * <p>Используется внутри Flink pipeline как этап очистки и нормализации данных.</p>
 */
public class FlatProfileMapFunction extends RichMapFunction<String, String> {

    private transient JsonEventProcessor processor;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        ObjectMapper mapper = new ObjectMapper();
        this.processor = new JsonEventProcessor(mapper);
    }

    @Override
    public String map(String json) {
        try {
            return processor.toFlatJson(json).orElse(null);
        } catch (Exception e) {
            return null;
        }
    }
}


