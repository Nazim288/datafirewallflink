package ru.gpbapp.datafirewallflink.rules;

import ru.gpbapp.datafirewallflink.rule.Rule;
import java.util.Map;
import java.util.regex.Pattern;

public class Rule1001 implements Rule {
    private boolean has(String s, String regex) {
        if (s == null) return false;
        return Pattern.compile(regex).matcher(s).find();
    }

    @Override
    public boolean apply(Map<String, String> data) {
        String name = data.get("client_name");
        return has(name, ".*[a-zA-Z].*") && has(name, ".*[а-яА-ЯёЁ].*");
    }
}

