package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import ru.gpbapp.datafirewallflink.mq.BrokerRecord;

import java.io.Serializable;

public class ArtemisSource implements Source<BrokerRecord, ArtemisSplit, Boolean>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final String inQueue;
    private final String user;
    private final String password;
    private final boolean logPayloads;
    private final int logPreviewLen;
    private final long receiveTimeoutMs;
    private final boolean tlsEnabled;
    private final String trustStorePath;
    private final String trustStorePassword;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String cipherSuite;

    public ArtemisSource(
            String host,
            int port,
            String inQueue,
            String user,
            String password,
            boolean logPayloads,
            int logPreviewLen,
            long receiveTimeoutMs,
            boolean tlsEnabled,
            String trustStorePath,
            String trustStorePassword,
            String keyStorePath,
            String keyStorePassword,
            String cipherSuite
    ) {
        this.host = host;
        this.port = port;
        this.inQueue = inQueue;
        this.user = user;
        this.password = password;
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.tlsEnabled = tlsEnabled;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.cipherSuite = cipherSuite;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<BrokerRecord, ArtemisSplit> createReader(SourceReaderContext readerContext) {
        return new ArtemisSourceReader(
                readerContext.getIndexOfSubtask(),
                host,
                port,
                inQueue,
                user,
                password,
                logPayloads,
                logPreviewLen,
                receiveTimeoutMs,
                tlsEnabled,
                trustStorePath,
                trustStorePassword,
                keyStorePath,
                keyStorePassword,
                cipherSuite
        );
    }

    @Override
    public SplitEnumerator<ArtemisSplit, Boolean> createEnumerator(SplitEnumeratorContext<ArtemisSplit> enumContext) {
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