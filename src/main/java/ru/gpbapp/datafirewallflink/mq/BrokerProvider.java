package ru.gpbapp.datafirewallflink.mq;

import ru.gpbapp.datafirewallflink.config.BrokerConfig;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface BrokerProvider {
    DataStream<BrokerRecord> buildSource(StreamExecutionEnvironment env, BrokerConfig config);
    void bindSink(DataStream<BrokerReply> stream, BrokerConfig config);
}
