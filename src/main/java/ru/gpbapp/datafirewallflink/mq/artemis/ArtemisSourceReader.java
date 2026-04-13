package ru.gpbapp.datafirewallflink.mq.artemis;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import ru.gpbapp.datafirewallflink.mq.BrokerRecord;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ArtemisSourceReader implements SourceReader<BrokerRecord, ArtemisSplit> {

    private static final Logger log = LoggerFactory.getLogger(ArtemisSourceReader.class);

    private final int subtaskId;
    private final String brokerUrl;
    private final String inQueue;
    private final String user;
    private final String password;
    private final boolean logPayloads;
    private final int logPreviewLen;
    private final long receiveTimeoutMs;

    private volatile boolean running = true;
    private boolean opened = false;
    private boolean splitAssigned = false;
    private boolean noMoreSplits = false;

    private transient ConnectionFactory connectionFactory;
    private transient Connection connection;
    private transient Session session;
    private transient Destination destination;
    private transient MessageConsumer consumer;

    public ArtemisSourceReader(
            int subtaskId,
            String brokerUrl,
            String inQueue,
            String user,
            String password,
            boolean logPayloads,
            int logPreviewLen,
            long receiveTimeoutMs
    ) {
        this.subtaskId = subtaskId;
        this.brokerUrl = brokerUrl;
        this.inQueue = inQueue;
        this.user = user;
        this.password = password;
        this.logPayloads = logPayloads;
        this.logPreviewLen = logPreviewLen;
        this.receiveTimeoutMs = receiveTimeoutMs;
    }

    @Override
    public void start() {
    }

    @Override
    public InputStatus pollNext(ReaderOutput<BrokerRecord> output) throws Exception {
        if (!running) {
            return InputStatus.END_OF_INPUT;
        }

        if (!splitAssigned) {
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        ensureOpen();

        Message msg = consumer.receive(receiveTimeoutMs);
        if (msg == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (!(msg instanceof TextMessage textMessage)) {
            log.warn("ArtemisSourceReader received non-TextMessage: {}", msg.getClass().getName());
            return InputStatus.MORE_AVAILABLE;
        }

        String body = textMessage.getText();
        String messageId = textMessage.getJMSMessageID();

        if (logPayloads) {
            log.info("ARTEMIS READ subtask={} messageId={} BODY:\n{}", subtaskId, messageId, body);
        } else {
            log.info(
                    "ARTEMIS READ subtask={} messageId={} BODY preview={}",
                    subtaskId, messageId, preview(body, logPreviewLen)
            );
        }

        output.collect(new BrokerRecord(messageId, body));
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<ArtemisSplit> snapshotState(long checkpointId) {
        if (splitAssigned) {
            return Collections.singletonList(new ArtemisSplit("artemis-split"));
        }
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<ArtemisSplit> splits) {
        if (splits != null && !splits.isEmpty()) {
            splitAssigned = true;
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() throws Exception {
        running = false;

        Exception first = null;

        try {
            if (consumer != null) consumer.close();
        } catch (Exception e) {
            first = e;
        }

        try {
            if (session != null) session.close();
        } catch (Exception e) {
            if (first == null) first = e;
        }

        try {
            if (connection != null) connection.close();
        } catch (Exception e) {
            if (first == null) first = e;
        }

        if (connectionFactory instanceof ActiveMQConnectionFactory cf) {
            try {
                cf.close();
            } catch (Exception e) {
                if (first == null) first = e;
            }
        }

        if (first != null) {
            throw first;
        }
    }

    private void ensureOpen() throws Exception {
        if (opened) {
            return;
        }

        log.info(
                "ArtemisSourceReader.open() subtask={} brokerUrl={} inQueue={} user={}",
                subtaskId, brokerUrl, inQueue, user
        );

        connectionFactory = new ActiveMQConnectionFactory(brokerUrl, user, password);
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createQueue(inQueue);
        consumer = session.createConsumer(destination);

        connection.start();
        opened = true;

        log.info(
                "ArtemisSourceReader opened inQueue={} subtask={} receiveTimeoutMs={} log.payloads={}",
                inQueue, subtaskId, receiveTimeoutMs, logPayloads
        );
    }

    private static String preview(String s, int max) {
        if (s == null) return "null";
        if (s.length() <= max) return s;
        return s.substring(0, max) + "...(+" + (s.length() - max) + " chars)";
    }
}
