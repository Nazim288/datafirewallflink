package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.kafka.RulesVersionEvent;
import ru.gpbapp.datafirewallflink.kafka.RulesVersionEventDeserializationSchema;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.services.MqWithRulesReloadBroadcastProcessFunction;

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

        JobConfig cfg = JobConfig.fromArgs(normalized);

        String mqUser = cfg.mqUser();
        String mqPassword = cfg.mqPassword();

        // --- MQ stream ---
        DataStream<MqRecord> mqStream = env.addSource(
                        new MqSource(
                                cfg.mqHost(),
                                cfg.mqPort(),
                                cfg.mqChannel(),
                                cfg.mqQmgr(),
                                cfg.mqInQueue(),
                                mqUser,
                                mqPassword
                        ),
                        "mq-source"
                )
                .name("mq-source")
                .setParallelism(1);

        // --- Kafka control stream ---
        // параметры через args:
        // --kafka.bootstrap=localhost:9092 --kafka.topic=dfw-rules --kafka.group=dfw-rules-group
        String bootstrap = pt.get("kafka.bootstrap", "localhost:9092");
        String topic = pt.get("kafka.topic", "dfw-rules");
        String group = pt.get("kafka.group", "dfw-rules-group");

        KafkaSource<RulesVersionEvent> kafkaSource = KafkaSource.<RulesVersionEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RulesVersionEventDeserializationSchema())
                .build();

        DataStream<RulesVersionEvent> updates = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "rules-kafka")
                .name("rules-kafka")
                .setParallelism(1);

                             // broadcast state: храним только последнюю примененную версию
        MapStateDescriptor<String, Long> bcDesc =
                new MapStateDescriptor<>("rules-version-broadcast", Types.STRING, Types.LONG);

        BroadcastStream<RulesVersionEvent> bcUpdates = updates.broadcast(bcDesc);

                            // --- connect MQ + broadcast updates ---
        DataStream<MqReply> processed = mqStream
                .connect(bcUpdates)
                .process(new MqWithRulesReloadBroadcastProcessFunction(bcDesc))
                .name("mq-process-with-rules-reload")
                .setParallelism(1); // можешь поднять, если MQ source позволит

        processed
                .filter(Objects::nonNull)
                .name("filter-non-null")
                .setParallelism(1)
                .addSink(new MqSink(
                        cfg.mqHost(),
                        cfg.mqPort(),
                        cfg.mqChannel(),
                        cfg.mqQmgr(),
                        cfg.mqOutQueue(),
                        mqUser,
                        mqPassword
                ))
                .name("mq-sink")
                .setParallelism(1);

        env.execute("DataFirewall IBM MQ Job (SHORT ANSWER) + Kafka Rules Reload");
    }

    private static String[] normalizeArgs(String[] args) {
        if (args == null || args.length == 0) return new String[0];
        List<String> out = new ArrayList<>();
        for (String a : args) {
            if (a == null) continue;
            if (a.startsWith("--") && a.contains("=")) {
                int idx = a.indexOf('=');
                out.add(a.substring(0, idx));
                out.add(a.substring(idx + 1));
            } else out.add(a);
        }
        return out.toArray(new String[0]);
    }
}
