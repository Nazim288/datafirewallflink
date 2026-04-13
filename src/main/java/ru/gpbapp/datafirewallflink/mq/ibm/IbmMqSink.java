package ru.gpbapp.datafirewallflink.mq.ibm;

import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import ru.gpbapp.datafirewallflink.mq.BrokerReply;
import ru.gpbapp.datafirewallflink.mq.MqConnect;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class IbmMqSink implements Sink<BrokerReply>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final String channel;
    private final String qmgr;
    private final String outQueue;
    private final String user;
    private final String password;

    public IbmMqSink(
            String host,
            int port,
            String channel,
            String qmgr,
            String outQueue,
            String user,
            String password
    ) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.qmgr = qmgr;
        this.outQueue = outQueue;
        this.user = user;
        this.password = password;
    }

    public IbmMqSink(
            String host,
            int port,
            String channel,
            String qmgr,
            String outQueue
    ) {
        this(host, port, channel, qmgr, outQueue, null, null);
    }

    /**
     * Bridge method for Flink 1.20.x compatibility.
     * Не ставим @Override специально.
     */
    @SuppressWarnings("deprecation")
    public SinkWriter<BrokerReply> createWriter(Sink.InitContext context) throws IOException {
        return new IbmMqSinkWriter(
                host,
                port,
                channel,
                qmgr,
                outQueue,
                user,
                password,
                context.getSubtaskId()
        );
    }

    @Override
    public SinkWriter<BrokerReply> createWriter(WriterInitContext context) throws IOException {
        return new IbmMqSinkWriter(
                host,
                port,
                channel,
                qmgr,
                outQueue,
                user,
                password,
                context.getSubtaskId()
        );
    }

    static final class IbmMqSinkWriter implements SinkWriter<BrokerReply> {

        private static final Logger log = LoggerFactory.getLogger(IbmMqSinkWriter.class);

        private final String host;
        private final int port;
        private final String channel;
        private final String qmgr;
        private final String outQueue;
        private final String user;
        private final String password;
        private final int subtaskId;

        private transient MQQueueManager qm;
        private transient MQQueue queue;

        IbmMqSinkWriter(
                String host,
                int port,
                String channel,
                String qmgr,
                String outQueue,
                String user,
                String password,
                int subtaskId
        ) throws IOException {
            this.host = host;
            this.port = port;
            this.channel = channel;
            this.qmgr = qmgr;
            this.outQueue = outQueue;
            this.user = user;
            this.password = password;
            this.subtaskId = subtaskId;

            open();
        }

        private void open() throws IOException {
            try {
                log.info(
                        "IbmMqSinkWriter.open() subtask={} connecting to {}:{} qmgr={} channel={} queue={} user={}",
                        subtaskId, host, port, qmgr, channel, outQueue, user
                );

                qm = MqConnect.connect(qmgr, host, port, channel, user, password);

                int openOptions = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
                queue = qm.accessQueue(outQueue, openOptions);

                log.info("IbmMqSinkWriter opened queue={} subtask={}", outQueue, subtaskId);
            } catch (Exception e) {
                throw new IOException("Failed to open IBM MQ sink writer", e);
            }
        }

        @Override
        public void write(BrokerReply value, Context context) throws IOException {
            if (value == null) {
                return;
            }

            try {
                String payload = value.payload;
                byte[] body = payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8);

                MQMessage msg = new MQMessage();
                msg.format = MQConstants.MQFMT_STRING;
                msg.characterSet = 1208;

                if (value.correlationIdBytes != null) {
                    msg.correlationId = BrokerReply.normalizeId(value.correlationIdBytes);
                } else if (value.correlationId != null) {
                    msg.correlationId = BrokerReply.toIbmMqBytes(value.correlationId);
                }

                if (body.length > 0) {
                    msg.write(body);
                }

                MQPutMessageOptions pmo = new MQPutMessageOptions();
                pmo.options = MQConstants.MQPMO_NO_SYNCPOINT | MQConstants.MQPMO_FAIL_IF_QUIESCING;

                queue.put(msg, pmo);
            } catch (Exception e) {
                throw new IOException("Failed to write message to IBM MQ", e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // Ничего не буферизуем.
        }

        @Override
        public void close() throws Exception {
            Exception first = null;

            try {
                if (queue != null) {
                    queue.close();
                }
            } catch (Exception e) {
                first = e;
            }

            try {
                if (qm != null) {
                    qm.disconnect();
                }
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                }
            }

            if (first != null) {
                throw first;
            }
        }
    }
}
