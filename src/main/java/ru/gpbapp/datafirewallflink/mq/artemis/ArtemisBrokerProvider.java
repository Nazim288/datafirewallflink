package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import ru.gpbapp.datafirewallflink.config.BrokerConfig;
import ru.gpbapp.datafirewallflink.mq.BrokerProvider;
import ru.gpbapp.datafirewallflink.mq.BrokerRecord;
import ru.gpbapp.datafirewallflink.mq.BrokerReply;

public class ArtemisBrokerProvider implements BrokerProvider {

    private final boolean logPayloads;
    private final int logPreviewLen;
    private final long receiveTimeoutMs;

    public ArtemisBrokerProvider() {
        this(false, 600, 1000);
    }

    public ArtemisBrokerProvider(boolean logPayloads, int logPreviewLen, long receiveTimeoutMs) {
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.receiveTimeoutMs = receiveTimeoutMs;
    }

    @Override
    public DataStream<BrokerRecord> buildSource(StreamExecutionEnvironment env, BrokerConfig config) {
        return env.fromSource(
                new ArtemisSource(
                        config.brokerUrl(),
                        config.inQueue(),
                        config.user(),
                        config.password(),
                        logPayloads,
                        logPreviewLen,
                        receiveTimeoutMs
                ),
                WatermarkStrategy.noWatermarks(),
                "artemis-source"
        ).setParallelism(1);
    }

    @Override
    public void bindSink(DataStream<BrokerReply> stream, BrokerConfig config) {
        stream.sinkTo(
                new ArtemisSink(
                        config.brokerUrl(),
                        config.outQueue(),
                        config.user(),
                        config.password()
                )
        ).name("artemis-sink").setParallelism(1);
    }
}
