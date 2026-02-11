package ru.gpbapp.datafirewallflink.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CompiledRulesResponse {

    private String cacheName;
    private String sourceName;

    @JsonAlias({"count", "size"})
    private int count;

    @JsonAlias({"classesBase64", "rules"})
    private Map<String, String> classesBase64;

    public CompiledRulesResponse() {}

    public String getCacheName() { return cacheName; }
    public String getSourceName() { return sourceName; }
    public int getCount() { return count; }

    public Map<String, String> getClassesBase64() { return classesBase64; }

    public Map<String, String> getRules() { return classesBase64; }

    public void setCacheName(String cacheName) { this.cacheName = cacheName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }
    public void setCount(int count) { this.count = count; }
    public void setClassesBase64(Map<String, String> classesBase64) { this.classesBase64 = classesBase64; }
}
