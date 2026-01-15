package ru.gpbapp.datafirewallflink.rules;

import ru.gpbapp.datafirewallflink.rule.Rule;
import java.util.Map;

public class Rule1002 implements Rule {

    @Override
    public boolean apply(Map<String, String> data) {
        try {
            int age = Integer.parseInt(data.get("age"));
            return age >= 18;
        } catch (Exception e) {
            return false;
        }
    }
}

