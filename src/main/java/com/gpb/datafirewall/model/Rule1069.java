package com.gpb.datafirewall.model;

import java.util.regex.Pattern;

public class Rule1069 implements Rule {
    private String toStringSafe(Object o) {
        return o == null ? null : o.toString()
                ;
    }

    private Double toDoubleOrNull(Object v) {
        try {
            return v == null ? null : Double.valueOf(v.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private int utf8Length(String s) {
        return s == null ? 0 : s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    private boolean regexp_like(String s, String pat) {
        if (s == null) return false;
        try {
            return Pattern.compile(pat).matcher(s).find();
        } catch (Exception e) {
            return false;
        }
    }

    public boolean apply(java.util.Map<String, String> data) {
        try {
            return (regexp_like(toStringSafe(data.get("ОСНОВНЫЕ СВЕДЕНИЯ.Имя")), toStringSafe(data.get(".*[a-zA-Z]+.*"))) && regexp_like(toStringSafe(data.get("ОСНОВНЫЕ СВЕДЕНИЯ.Имя")), toStringSafe(data.get(".*[а-яА-ЯеЁ]+.*"))));
        } catch (Exception e) {
            return false;
        }
    }
}


