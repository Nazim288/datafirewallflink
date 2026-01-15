package ru.gpbapp.datafirewallflink.validation;

import java.util.Objects;

/**
 * ruleName: ключ в registry (например "Rule1069")
 * section/field: куда писать статус в short ANSWER (details.baseInfo.name)
 * logicalFieldKey: логическое поле для ANSWER_DETAIL (например "ОСНОВНЫЕ СВЕДЕНИЯ.Имя")
 */
public final class FieldRuleBinding {
    public final String ruleName;
    public final String section;
    public final String field;
    public final String logicalFieldKey;

    public FieldRuleBinding(String ruleName, String section, String field, String logicalFieldKey) {
        this.ruleName = Objects.requireNonNull(ruleName);
        this.section = Objects.requireNonNull(section);
        this.field = Objects.requireNonNull(field);
        this.logicalFieldKey = Objects.requireNonNull(logicalFieldKey);
    }
}

