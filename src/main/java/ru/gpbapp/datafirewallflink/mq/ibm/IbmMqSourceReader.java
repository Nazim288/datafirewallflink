package ru.gpbapp.datafirewallflink.mq.ibm;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import ru.gpbapp.datafirewallflink.mq.BrokerRecord;
import ru.gpbapp.datafirewallflink.mq.MqConnect;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class IbmMqSourceReader implements SourceReader<BrokerRecord, IbmMqSplit> {

    private static final Logger log = LoggerFactory.getLogger(IbmMqSourceReader.class);

    private static final Map<Integer, Charset> CCSID_MAP = Map.ofEntries(
            Map.entry(1208, StandardCharsets.UTF_8),
            Map.entry(1200, StandardCharsets.UTF_16),
            Map.entry(819, StandardCharsets.ISO_8859_1),
            Map.entry(1251, Charset.forName("windows-1251")),
            Map.entry(1252, Charset.forName("windows-1252")),
            Map.entry(866, Charset.forName("Cp866")),
            Map.entry(850, Charset.forName("Cp850"))
    );

    private final int subtaskId;
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

    private volatile boolean running = true;
    private boolean opened = false;
    private boolean splitAssigned = false;
    private boolean noMoreSplits = false;

    private transient MQQueueManager qMgr;
    private transient MQQueue queue;

    public IbmMqSourceReader(
            int subtaskId,
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
        this.subtaskId = subtaskId;
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
    public void start() {
        // split запросится автоматически через isAvailable/pollNext
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

        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = waitIntervalMs;

        MQMessage msg = new MQMessage();

        try {
            queue.get(msg, gmo);

            int messageLength = msg.getMessageLength();
            int dataLength = msg.getDataLength();

            log.debug(
                    "MQ message props: ccsid={} encoding={} format={} dataLength={} msgLength={}",
                    msg.characterSet, msg.encoding, msg.format, dataLength, messageLength
            );

            byte[] buf = new byte[dataLength];
            msg.readFully(buf);

            Charset cs = detectCharset(msg);
            String body = new String(buf, cs);

            byte[] msgIdBytes = Arrays.copyOf(msg.messageId, msg.messageId.length);
            String msgIdHex = toHexSafe(msgIdBytes);
            boolean endsWithJsonClose = body != null && body.trim().endsWith("}");

            log.info(
                    "MQ READ subtask={} msgIdLen={} messageLength={} dataLength={} bytesRead={} bodyChars={} endsWithJsonClose={}",
                    subtaskId,
                    msgIdBytes.length,
                    messageLength,
                    dataLength,
                    buf.length,
                    body != null ? body.length() : 0,
                    endsWithJsonClose
            );

            if (logPayloads) {
                log.info("MQ READ msgId={} BODY:\n{}", msgIdHex, body);
            } else {
                log.info("MQ READ msgId={} BODY preview={}", msgIdHex, preview(body, logPreviewLen));
            }

            output.collect(new BrokerRecord(msgIdBytes, body));
            return InputStatus.MORE_AVAILABLE;

        } catch (MQException mqe) {
            if (mqe.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            log.error(
                    "MQ GET failed (completionCode={}, reasonCode={})",
                    mqe.completionCode,
                    mqe.reasonCode,
                    mqe
            );
            throw mqe;
        }
    }

    @Override
    public List<IbmMqSplit> snapshotState(long checkpointId) {
        if (splitAssigned) {
            return Collections.singletonList(new IbmMqSplit("ibm-mq-split"));
        }
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<IbmMqSplit> splits) {
        if (splits != null && !splits.isEmpty()) {
            splitAssigned = true;
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() {
        running = false;

        try {
            if (queue != null) {
                queue.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close MQQueue", e);
        }

        try {
            if (qMgr != null) {
                qMgr.disconnect();
            }
        } catch (Exception e) {
            log.warn("Failed to disconnect MQQueueManager", e);
        }
    }

    private void ensureOpen() throws Exception {
        if (opened) {
            return;
        }

        int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING;

        log.info(
                "IbmMqSourceReader.open() subtask={} connecting to {}:{} qmgr={} channel={} queue={} user={}",
                subtaskId, host, port, qmgr, channel, queueName, user
        );

        qMgr = MqConnect.connect(qmgr, host, port, channel, user, password);
        queue = qMgr.accessQueue(queueName, openOptions);

        opened = true;

        log.info(
                "IbmMqSourceReader opened queue={} subtask={} log.payloads={} log.preview.len={} wait.ms={}",
                queueName, subtaskId, logPayloads, logPreviewLen, waitIntervalMs
        );
    }

    private Charset detectCharset(MQMessage msg) {
        int ccsid = msg.characterSet;

        if (ccsid <= 0) {
            return StandardCharsets.UTF_8;
        }

        Charset mapped = CCSID_MAP.get(ccsid);
        if (mapped != null) {
            return mapped;
        }

        Charset cs = tryCharset("Cp" + ccsid);
        if (cs != null) return cs;

        cs = tryCharset("IBM" + ccsid);
        if (cs != null) return cs;

        cs = tryCharset("x-IBM" + ccsid);
        if (cs != null) return cs;

        log.debug("Unknown CCSID={} -> fallback UTF-8", ccsid);
        return StandardCharsets.UTF_8;
    }

    private Charset tryCharset(String name) {
        if (name == null) return null;

        String n = name.trim();
        if (n.isEmpty()) return null;

        boolean hasDigit = false;
        for (int i = 0; i < n.length(); i++) {
            if (Character.isDigit(n.charAt(i))) {
                hasDigit = true;
                break;
            }
        }
        if (!hasDigit) {
            log.debug("Skip charset candidate (no digits): '{}'", n);
            return null;
        }

        try {
            return Charset.forName(n);
        } catch (Exception e) {
            log.debug("Charset not supported: '{}'", n);
            return null;
        }
    }

    private static String preview(String s, int max) {
        if (s == null) return "null";
        if (s.length() <= max) return s;
        return s.substring(0, max) + "...(+" + (s.length() - max) + " chars)";
    }

    private static String toHexSafe(byte[] bytes) {
        if (bytes == null) return "null";

        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02X", b));
        }
        return sb.toString();
    }
}
