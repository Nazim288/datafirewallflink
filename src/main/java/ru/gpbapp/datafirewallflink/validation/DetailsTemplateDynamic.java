package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

import static ru.gpbapp.datafirewallflink.validation.DetailsTemplateValues.STRING;

public final class DetailsTemplateDynamic {

    private final ObjectMapper mapper;

    public DetailsTemplateDynamic(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode createDetailsSkeleton(List<FieldRuleBinding> bindings) {
        if (bindings == null) bindings = List.of();

        ObjectNode details = mapper.createObjectNode();
        details.put("ALL_RESULT", STRING);

        boolean hasBase = bindings.stream().anyMatch(b -> "baseInfo".equals(b.section));
        boolean hasDocs = bindings.stream().anyMatch(b -> "documents".equals(b.section));
        boolean hasCard0 = bindings.stream().anyMatch(b -> "clientIdCard0".equals(b.section));

        if (hasBase) {
            ObjectNode baseInfo = mapper.createObjectNode();
            baseInfo.put("dataset_code", STRING);

            bindings.stream()
                    .filter(b -> "baseInfo".equals(b.section))
                    .forEach(b -> {
                        if (b != null && b.field != null && !baseInfo.has(b.field)) {
                            baseInfo.put(b.field, STRING);
                        }
                    });

            details.set("baseInfo", baseInfo);
        }

        if (hasDocs || hasCard0) {
            ObjectNode documents = mapper.createObjectNode();
            documents.put("dataset_code", STRING);

            if (hasDocs) {
                bindings.stream()
                        .filter(b -> "documents".equals(b.section))
                        .forEach(b -> {
                            if (b != null && b.field != null && !documents.has(b.field)) {
                                documents.put(b.field, STRING);
                            }
                        });
            }

            if (hasCard0) {
                ArrayNode arr = mapper.createArrayNode();
                ObjectNode card0 = mapper.createObjectNode();
                card0.put("elemId", STRING);

                bindings.stream()
                        .filter(b -> "clientIdCard0".equals(b.section))
                        .forEach(b -> {
                            if (b != null && b.field != null && !card0.has(b.field)) {
                                card0.put(b.field, STRING);
                            }
                        });

                arr.add(card0);
                documents.set("clientIdCard", arr);
            }

            details.set("documents", documents);
        }

        return details;
    }
}
