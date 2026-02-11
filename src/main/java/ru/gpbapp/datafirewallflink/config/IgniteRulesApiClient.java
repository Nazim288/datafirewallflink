package ru.gpbapp.datafirewallflink.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.gpbapp.datafirewallflink.dto.CompiledRulesResponse;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class IgniteRulesApiClient {

    private static final ObjectMapper OM = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String baseUrl;
    private final HttpClient http;

    public IgniteRulesApiClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    public CompiledRulesResponse getCompiledRules(String sourceName) throws Exception {
        String url = baseUrl + "/api/compiled-rules?sourceName=" +
                URLEncoder.encode(sourceName, StandardCharsets.UTF_8);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .GET()
                .build();

        try {
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException("Ignite API HTTP " + resp.statusCode() + " for " + url
                        + ": " + truncate(resp.body(), 800));
            }

            return OM.readValue(resp.body(), CompiledRulesResponse.class);

        } catch (Exception e) {
            throw new RuntimeException("Failed to call Ignite API: " + url, e);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
