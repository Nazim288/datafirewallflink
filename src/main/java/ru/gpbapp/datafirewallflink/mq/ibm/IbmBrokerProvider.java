package ru.gpbapp.datafirewallflink.mq.ibm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import ru.gpbapp.datafirewallflink.config.BrokerConfig;
import ru.gpbapp.datafirewallflink.mq.BrokerRecord;
import ru.gpbapp.datafirewallflink.mq.BrokerReply;

public class IbmBrokerProvider implements ru.gpbapp.datafirewallflink.mq.BrokerProvider {

    private final boolean logPayloads;
    private final int logPreviewLen;
    private final int waitIntervalMs;

    public IbmBrokerProvider() {
        this(false, 600, 1000);
    }

    public IbmBrokerProvider(boolean logPayloads, int logPreviewLen, int waitIntervalMs) {
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.waitIntervalMs = waitIntervalMs;
    }

    @Override
    public DataStream<BrokerRecord> buildSource(StreamExecutionEnvironment env, BrokerConfig config) {
        return env.fromSource(
                new IbmMqSource(
                        config.host(),
                        config.port(),
                        config.channel(),
                        config.qmgr(),
                        config.inQueue(),
                        config.user(),
                        config.password(),
                        logPayloads,
                        logPreviewLen,
                        waitIntervalMs
                ),
                WatermarkStrategy.noWatermarks(),
                "ibm-mq-source"
        ).setParallelism(1);
    }

    @Override
    public void bindSink(DataStream<BrokerReply> stream, BrokerConfig config) {
        stream.sinkTo(
                new IbmMqSink(
                        config.host(),
                        config.port(),
                        config.channel(),
                        config.qmgr(),
                        config.outQueue(),
                        config.user(),
                        config.password()
                )
        ).name("ibm-mq-sink").setParallelism(1);
    }
}
