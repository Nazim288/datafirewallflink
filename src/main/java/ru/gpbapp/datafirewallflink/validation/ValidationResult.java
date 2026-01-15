package ru.gpbapp.datafirewallflink.validation;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

public record ValidationResult(
        ObjectNode details,                    // для short
        String allResult,                      // SUCCESS/ERROR
        String processStatus,                  // OK/ERROR
        Map<String, Map<String, String>> detailByField // logicalField -> (ruleName -> SUCCESS/ERROR)
) {}

