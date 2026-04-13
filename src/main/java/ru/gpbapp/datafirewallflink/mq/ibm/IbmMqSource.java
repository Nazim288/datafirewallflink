package ru.gpbapp.datafirewallflink.mq.ibm;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import ru.gpbapp.datafirewallflink.mq.BrokerRecord;

import java.io.Serializable;

public class IbmMqSource implements Source<BrokerRecord, IbmMqSplit, Boolean>, Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean logPayloads;
    private final int logPreviewLen;

    private final String host;
    private final int port;
    private final String channel;
    private final String qmgr;
    private final String queueName;
    private final String user;
    private final String password;
    private final int waitIntervalMs;

    public IbmMqSource(
            String host,
            int port,
            String channel,
            String qmgr,
            String queueName,
            String user,
            String password
    ) {
        this(host, port, channel, qmgr, queueName, user, password, false, 600, 1000);
    }

    public IbmMqSource(
            String host,
            int port,
            String channel,
            String qmgr,
            String queueName,
            String user,
            String password,
            boolean logPayloads,
            int logPreviewLen,
            int waitIntervalMs
    ) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.queueName = queueName;
        this.user = user;
        this.password = password;
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.waitIntervalMs = waitIntervalMs;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<BrokerRecord, IbmMqSplit> createReader(SourceReaderContext readerContext) {
        return new IbmMqSourceReader(
                readerContext.getIndexOfSubtask(),
                host,
                port,
                channel,
                qmgr,
                queueName,
                user,
                password,
                logPayloads,
                logPreviewLen,
                waitIntervalMs
        );
    }

    @Override
    public SplitEnumerator<IbmMqSplit, Boolean> createEnumerator(
            SplitEnumeratorContext<IbmMqSplit> enumContext
    ) {
        return new IbmMqSplitEnumerator(enumContext, false);
    }

    @Override
    public SplitEnumerator<IbmMqSplit, Boolean> restoreEnumerator(
            SplitEnumeratorContext<IbmMqSplit> enumContext,
            Boolean checkpoint
    ) {
        return new IbmMqSplitEnumerator(enumContext, Boolean.TRUE.equals(checkpoint));
    }

    @Override
    public SimpleVersionedSerializer<IbmMqSplit> getSplitSerializer() {
        return new IbmMqSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
        return new IbmMqEnumeratorStateSerializer();
    }
}
