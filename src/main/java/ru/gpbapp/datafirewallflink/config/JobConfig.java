package ru.gpbapp.datafirewallflink.config;

public record JobConfig(String inputPath) {
    public static JobConfig fromArgs(String[] args) {
        String inputPath = args.length > 0 ? args[0] : "input/events.jsonl";
        return new JobConfig(inputPath);
    }
}

