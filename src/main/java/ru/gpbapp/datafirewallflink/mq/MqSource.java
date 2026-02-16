package ru.gpbapp.datafirewallflink.mq;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class MqSource extends RichParallelSourceFunction<MqRecord> {
    private static final Logger log = LoggerFactory.getLogger(MqSource.class);
    private volatile boolean running = true;

    private final String host;
    private final int port;
    private final String channel;
    private final String qmgr;
    private final String inQueue;

    // ✅ добавили
    private final String user;
    private final String password;

    private transient MQQueueManager qm;
    private transient MQQueue queue;

    // ✅ старый конструктор оставим, чтобы ничего не сломать
    public MqSource(String host, int port, String channel, String qmgr, String inQueue) {
        this(host, port, channel, qmgr, inQueue, null, null);
    }

    // ✅ новый конструктор с кредами
    public MqSource(String host, int port, String channel, String qmgr, String inQueue,
                    String user, String password) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.inQueue = inQueue;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        log.info("MqSource.open() subtask={} connecting to {}:{} qmgr={} channel={} queue={} user={}",
                subtask, host, port, qmgr, channel, inQueue, user);

        // ✅ ВАЖНО: теперь коннектимся с user/password
        qm = MqConnect.connect(qmgr, host, port, channel, user, password);

        int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING;
        queue = qm.accessQueue(inQueue, openOptions);

        log.info("MqSource opened queue={} subtask={}", inQueue, subtask);
    }

    @Override
    public void run(SourceContext<MqRecord> ctx) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = 1000;

        while (running) {
            try {
                MQMessage msg = new MQMessage();
                queue.get(msg, gmo);

                byte[] data = new byte[msg.getDataLength()];
                msg.readFully(data);

                String body = new String(data, StandardCharsets.UTF_8);
                byte[] msgIdCopy = Arrays.copyOf(msg.messageId, msg.messageId.length);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new MqRecord(msgIdCopy, body));
                }

            } catch (MQException e) {
                if (!running) break;

                if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
                    log.error("MQ error in source subtask={} reasonCode={} msg={}",
                            subtask, e.reasonCode, e.getMessage(), e);
                    throw e;
                }
            }
        }

        log.info("MqSource stopped subtask={}", subtask);
    }

    @Override
    public void cancel() {
        running = false;
        try { if (queue != null) queue.close(); } catch (Exception ignore) {}
        try { if (qm != null) qm.disconnect(); } catch (Exception ignore) {}
    }

    @Override
    public void close() {
        try { if (queue != null) queue.close(); } catch (Exception ignore) {}
        try { if (qm != null) qm.disconnect(); } catch (Exception ignore) {}
    }
}
