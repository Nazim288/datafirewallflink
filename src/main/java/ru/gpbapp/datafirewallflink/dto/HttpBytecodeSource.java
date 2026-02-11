package ru.gpbapp.datafirewallflink.dto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.IgniteRulesApiClient;
import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class HttpBytecodeSource implements BytecodeSource {
    private static final Logger log = LoggerFactory.getLogger(HttpBytecodeSource.class);

    private final IgniteRulesApiClient apiClient;

    public HttpBytecodeSource(IgniteRulesApiClient apiClient) {
        this.apiClient = Objects.requireNonNull(apiClient, "apiClient");
    }

    @Override
    public Map<String, byte[]> loadAll(String sourceName) {

        try {
            CompiledRulesResponse resp = apiClient.getCompiledRules(sourceName);
            Map<String, String> rules = (resp == null) ? null : resp.getRules();

            if (rules == null || rules.isEmpty()) {
                return Map.of();
            }

            Base64.Decoder dec = Base64.getDecoder();
            Map<String, byte[]> out = new LinkedHashMap<>();

            rules.entrySet().stream()
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
            log.info("[RULES] http response cacheName={} sourceName={} count={}",
                    resp.getCacheName(), resp.getSourceName(), resp.getCount());

            return out;

        } catch (Exception e) {
            throw new RuntimeException("Failed to load compiled rules via HTTP for sourceName=" + sourceName, e);
        }
    }
}
