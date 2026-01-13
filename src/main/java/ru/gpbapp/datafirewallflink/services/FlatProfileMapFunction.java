package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;

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


