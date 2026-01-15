package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.gpbapp.datafirewallflink.validation.AnswerBuilder;
import ru.gpbapp.datafirewallflink.validation.ValidationResult;

/**
 * Сервис формирования короткого ответа (ANSWER).
 */
public final class ShortAnswerService {

    private final ObjectMapper mapper;
    private final AnswerBuilder answerBuilder;

    public ShortAnswerService(ObjectMapper mapper) {
        this.mapper = mapper;
        this.answerBuilder = new AnswerBuilder(mapper);
    }

    /**
     * @param originalEvent исходное событие (QUERY)
     * @param validation    результат валидации (details + ALL_RESULT + status)
     * @return JSON-строка ответа ANSWER или null при ошибке
     */
    public String build(JsonNode originalEvent, ValidationResult validation) {
        try {
            ObjectNode node = answerBuilder.buildAnswer(originalEvent, validation);
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            return null;
        }
    }
}
