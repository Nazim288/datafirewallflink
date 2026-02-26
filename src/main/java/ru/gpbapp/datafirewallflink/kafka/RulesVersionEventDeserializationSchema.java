package ru.gpbapp.datafirewallflink.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.nio.charset.StandardCharsets;

public class RulesVersionEventDeserializationSchema extends AbstractDeserializationSchema<RulesVersionEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public RulesVersionEvent deserialize(byte[] message) {
        try {
            String s = new String(message, StandardCharsets.UTF_8);
            JsonNode n = MAPPER.readTree(s);
            long v = n.path("version").asLong(-1);
            return new RulesVersionEvent(v);
        } catch (Exception e) {
            return new RulesVersionEvent(-1);
        }
    }
}

