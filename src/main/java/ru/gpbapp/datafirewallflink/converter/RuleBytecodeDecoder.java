package ru.gpbapp.datafirewallflink.converter;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public final class RuleBytecodeDecoder {

    private RuleBytecodeDecoder() {}

    public static Map<String, byte[]> decodeToBytes(Map<String, String> base64ByKey) {
        if (base64ByKey == null || base64ByKey.isEmpty()) return Map.of();

        Base64.Decoder dec = Base64.getDecoder();
        Map<String, byte[]> out = new LinkedHashMap<>();

        base64ByKey.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    String key = e.getKey();
                    String b64 = e.getValue();

                    if (key == null || key.isBlank() || b64 == null || b64.isBlank()) {
                        return;
                    }

                    try {
                        out.put(key, dec.decode(b64));
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException("Invalid base64 for key='" + key + "'", ex);
                    }
                });

        return out;
    }
}
