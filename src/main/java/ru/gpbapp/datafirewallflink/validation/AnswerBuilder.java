package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.List;

public final class AnswerBuilder {

    private final ObjectMapper mapper;

    public AnswerBuilder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode buildAnswer(JsonNode originalEvent,
                                  ValidationResult validation) {

        ObjectNode out = mapper.createObjectNode();

        out.set("details", validation.details());

        copyIfExists(originalEvent, out, List.of(
                "dfw_query_id",
                "dfw_hostname",
                "dfw_user_login",
                "dfw_dataset_code",
                "dfw_readed_from_mq_dttm"
        ));

        out.put("dfw_action_type", "ANSWER");
        out.put("PROCESS_STATUS", validation.processStatus());

        String now = Instant.now().toString();
        out.put("dfw_created_dttm", now);
        out.put("dfw_action_dttm", now);

        return out;
    }

    private static void copyIfExists(JsonNode src, ObjectNode dst, List<String> fields) {
        for (String f : fields) {
            JsonNode v = src.get(f);
            if (v != null && !v.isNull()) dst.set(f, v);
        }
    }
}

