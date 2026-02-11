package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.services.ShortAnswerMapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        String[] normalized = normalizeArgs(args);
        ParameterTool pt = ParameterTool.fromArgs(normalized);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(pt);

        int p = pt.getInt("parallelism", 1);
        env.setParallelism(p);

        log.info("[MAIN] parallelism={}", p);
        log.info("[MAIN] rules.loader={}", pt.get("rules.loader", "<not set>"));
        log.info("[MAIN] rules.sourceName={}", pt.get("rules.sourceName", "<not set>"));
        log.info("[MAIN] ignite.apiUrl={}", pt.get("ignite.apiUrl", "<not set>"));
        log.info("[MAIN] ignite.host={}", pt.get("ignite.host", "<not set>"));
        log.info("[MAIN] ignite.port={}", pt.get("ignite.port", "<not set>"));

        JobConfig cfg = JobConfig.fromArgs(normalized);

        env.addSource(
                        new MqSource(
                                cfg.mqHost(),
                                cfg.mqPort(),
                                cfg.mqChannel(),
                                cfg.mqQmgr(),
                                cfg.mqInQueue()
                        ),
                        "mq-source"
                )
                .name("mq-source")
                .setParallelism(1)

                .map(new ShortAnswerMapFunction())
                .name("short-answer-map")
                .setParallelism(1)

                .filter(Objects::nonNull)
                .name("filter-non-null")
                .setParallelism(1)

                .addSink(
                        new MqSink(
                                cfg.mqHost(),
                                cfg.mqPort(),
                                cfg.mqChannel(),
                                cfg.mqQmgr(),
                                cfg.mqOutQueue()
                        )
                )
                .name("mq-sink")
                .setParallelism(1);

        env.execute("DataFirewall IBM MQ Job (SHORT ANSWER)");
    }

    /**
     * Превращает аргументы вида:
     *   --a=b --c=d
     * в:
     *   --a b --c d
     *
     * Это максимально совместимо с ParameterTool.fromArgs().
     */
    private static String[] normalizeArgs(String[] args) {
        if (args == null || args.length == 0) return new String[0];

        List<String> out = new ArrayList<>();
        for (String a : args) {
            if (a == null) continue;

            if (a.startsWith("--") && a.contains("=")) {
                int idx = a.indexOf('=');
                String k = a.substring(0, idx);
                String v = a.substring(idx + 1);
                out.add(k);
                out.add(v);
            } else {
                out.add(a);
            }
        }
        return out.toArray(new String[0]);
    }
}
