package ru.gpbapp.datafirewallflink.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.converter.EventToFlatProfile;

import java.util.Optional;

public class JsonEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(JsonEventProcessor.class);

    private final ObjectMapper mapper;
    private final EventToFlatProfile converter;

    public JsonEventProcessor(ObjectMapper mapper) {
        this.mapper = mapper;
        this.converter = new EventToFlatProfile(mapper);
    }

    /** @return Optional.empty() если строка пустая/битая */
    public Optional<String> toFlatJson(String jsonLine) {
        if (jsonLine == null) {
            return Optional.empty();
        }
        String s = jsonLine.trim();
        if (s.isEmpty()) {
            return Optional.empty();
        }

        try {
            JsonNode event = mapper.readTree(s);
            JsonNode flat = converter.convert(event);
            return Optional.of(mapper.writeValueAsString(flat));

        } catch (JsonProcessingException e) {
            // битый JSON / неожиданный формат
            log.debug("Skip invalid json line: {}", safeSnippet(s), e);
            return Optional.empty();

        } catch (Exception e) {
            // конвертер/логика упала
            log.warn("Skip line due to processing error: {}", safeSnippet(s), e);
            return Optional.empty();
        }
    }

    private static String safeSnippet(String s) {
        // чтобы логи не раздувались и не утекали данные целиком
        int max = 200;
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}

