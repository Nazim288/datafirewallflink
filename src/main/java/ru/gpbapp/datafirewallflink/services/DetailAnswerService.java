package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.gpbapp.datafirewallflink.validation.DetailAnswerBuilder;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

public final class DetailAnswerService {

    private final ObjectMapper mapper;
    private final DetailAnswerBuilder builder;

    public DetailAnswerService(ObjectMapper mapper) {
        this.mapper = mapper;
        this.builder = new DetailAnswerBuilder(mapper);
    }

    public String build(JsonNode originalEvent, ValidationResult validation) {
        try {
            ObjectNode node = builder.buildDetailAnswer(
                    originalEvent,
                    validation.allResult(),
                    validation.detailByField()
            );
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            return null;
        }
    }
}

