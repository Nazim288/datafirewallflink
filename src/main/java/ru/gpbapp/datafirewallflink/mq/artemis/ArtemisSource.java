package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import ru.gpbapp.datafirewallflink.mq.BrokerRecord;

import java.io.Serializable;

public class ArtemisSource implements Source<BrokerRecord, ArtemisSplit, Boolean>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerUrl;
    private final String inQueue;
    private final String user;
    private final String password;
    private final boolean logPayloads;
    private final int logPreviewLen;
    private final long receiveTimeoutMs;

    public ArtemisSource(
            String brokerUrl,
            String inQueue,
            String user,
            String password,
            boolean logPayloads,
            int logPreviewLen,
            long receiveTimeoutMs
    ) {
        this.brokerUrl = brokerUrl;
        this.inQueue = inQueue;
        this.user = user;
        this.password = password;
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.receiveTimeoutMs = receiveTimeoutMs;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<BrokerRecord, ArtemisSplit> createReader(SourceReaderContext readerContext) {
        return new ArtemisSourceReader(
                readerContext.getIndexOfSubtask(),
                brokerUrl,
                inQueue,
                user,
                password,
                logPayloads,
                logPreviewLen,
                receiveTimeoutMs
        );
    }

    @Override
    public SplitEnumerator<ArtemisSplit, Boolean> createEnumerator(
            SplitEnumeratorContext<ArtemisSplit> enumContext
    ) {
        return new ArtemisSplitEnumerator(enumContext, false);
    }

    @Override
    public SplitEnumerator<ArtemisSplit, Boolean> restoreEnumerator(
            SplitEnumeratorContext<ArtemisSplit> enumContext,
            Boolean checkpoint
    ) {
        return new ArtemisSplitEnumerator(enumContext, Boolean.TRUE.equals(checkpoint));
    }

    @Override
    public SimpleVersionedSerializer<ArtemisSplit> getSplitSerializer() {
        return new ArtemisSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
        return new ArtemisEnumeratorStateSerializer();
    }
}