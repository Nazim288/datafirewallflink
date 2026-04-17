package ru.gpbapp.datafirewallflink.mq.artemis;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.mq.BrokerReply;

import java.io.IOException;
import java.io.Serializable;

public class ArtemisSink implements Sink<BrokerReply>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final String outQueue;
    private final String user;
    private final String password;

    private final boolean tlsEnabled;
    private final String trustStorePath;
    private final String trustStorePassword;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String cipherSuite;

    public ArtemisSink(
            String host,
            int port,
            String outQueue,
            String user,
            String password,
            boolean tlsEnabled,
            String trustStorePath,
            String trustStorePassword,
            String keyStorePath,
            String keyStorePassword,
            String cipherSuite
    ) {
        this.host = host;
        this.port = port;
        this.outQueue = outQueue;
        this.user = user;
        this.password = password;
        this.tlsEnabled = tlsEnabled;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.cipherSuite = cipherSuite;
    }

    @SuppressWarnings("deprecation")
    public SinkWriter<BrokerReply> createWriter(Sink.InitContext context) throws IOException {
        return new ArtemisSinkWriter(
                host,
                port,
                outQueue,
                user,
                password,
                tlsEnabled,
                trustStorePath,
                trustStorePassword,
                keyStorePath,
                keyStorePassword,
                cipherSuite,
                context.getSubtaskId()
        );
    }

    @Override
    public SinkWriter<BrokerReply> createWriter(WriterInitContext context) throws IOException {
        return new ArtemisSinkWriter(
                host,
                port,
                outQueue,
                user,
                password,
                tlsEnabled,
                trustStorePath,
                trustStorePassword,
                keyStorePath,
                keyStorePassword,
                cipherSuite,
                context.getSubtaskId()
        );
    }

    static final class ArtemisSinkWriter implements SinkWriter<BrokerReply> {

        private static final Logger log = LoggerFactory.getLogger(ArtemisSinkWriter.class);

        private final String host;
        private final int port;
        private final String outQueue;
        private final String user;
        private final String password;
        private final boolean tlsEnabled;
        private final String trustStorePath;
        private final String trustStorePassword;
        private final String keyStorePath;
        private final String keyStorePassword;
        private final String cipherSuite;
        private final int subtaskId;

        private transient ConnectionFactory connectionFactory;
        private transient Connection connection;
        private transient Session session;
        private transient Destination destination;
        private transient MessageProducer producer;

        ArtemisSinkWriter(
                String host,
                int port,
                String outQueue,
                String user,
                String password,
                boolean tlsEnabled,
                String trustStorePath,
                String trustStorePassword,
                String keyStorePath,
                String keyStorePassword,
                String cipherSuite,
                int subtaskId
        ) throws IOException {
            this.host = host;
            this.port = port;
            this.outQueue = outQueue;
            this.user = user;
            this.password = password;
            this.tlsEnabled = tlsEnabled;
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
            this.keyStorePath = keyStorePath;
            this.keyStorePassword = keyStorePassword;
            this.cipherSuite = cipherSuite;
            this.subtaskId = subtaskId;

            open();
        }

        private void open() throws IOException {
            try {
                String brokerUrl = ArtemisConnectionUrlBuilder.build(
                        host,
                        port,
                        tlsEnabled,
                        trustStorePath,
                        trustStorePassword,
                        keyStorePath,
                        keyStorePassword,
                        cipherSuite
                );

                log.info(
                        "ArtemisSinkWriter.open() subtask={} brokerUrl={} outQueue={} user={} tlsEnabled={}",
                        subtaskId, brokerUrl, outQueue, user, tlsEnabled
                );

                connectionFactory = new ActiveMQConnectionFactory(brokerUrl, user, password);
                connection = connectionFactory.createConnection();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                destination = session.createQueue(outQueue);
                producer = session.createProducer(destination);

                connection.start();

                log.info("ArtemisSinkWriter opened outQueue={} subtask={}", outQueue, subtaskId);
            } catch (Exception e) {
                throw new IOException("Failed to open Artemis sink writer", e);
            }
        }

        @Override
        public void write(BrokerReply value, Context context) throws IOException {
            if (value == null) {
                return;
            }

            try {
                String payload = value.payload == null ? "" : value.payload;
                TextMessage message = session.createTextMessage(payload);

                if (value.correlationId != null && !value.correlationId.isBlank()) {
                    message.setJMSCorrelationID(value.correlationId);
                }

                producer.send(message);
            } catch (Exception e) {
                throw new IOException("Failed to write message to Artemis", e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
        }

        @Override
        public void close() throws Exception {
            Exception first = null;

            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (Exception e) {
                first = e;
            }

            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                }
            }

            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                }
            }

            if (connectionFactory instanceof ActiveMQConnectionFactory cf) {
                try {
                    cf.close();
                } catch (Exception e) {
                    if (first == null) {
                        first = e;
                    }
                }
            }

            if (first != null) {
                throw first;
            }
        }
    }
}