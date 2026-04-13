package ru.gpbapp.datafirewallflink.mq.artemis;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import ru.gpbapp.datafirewallflink.mq.BrokerReply;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class ArtemisSink implements Sink<BrokerReply>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerUrl;
    private final String outQueue;
    private final String user;
    private final String password;

    public ArtemisSink(String brokerUrl, String outQueue, String user, String password) {
        this.brokerUrl = brokerUrl;
        this.outQueue = outQueue;
        this.user = user;
        this.password = password;
    }

    @SuppressWarnings("deprecation")
    public SinkWriter<BrokerReply> createWriter(Sink.InitContext context) throws IOException {
        return new ArtemisSinkWriter(
                brokerUrl,
                outQueue,
                user,
                password,
                context.getSubtaskId()
        );
    }

    @Override
    public SinkWriter<BrokerReply> createWriter(WriterInitContext context) throws IOException {
        return new ArtemisSinkWriter(
                brokerUrl,
                outQueue,
                user,
                password,
                context.getSubtaskId()
        );
    }

    static final class ArtemisSinkWriter implements SinkWriter<BrokerReply> {

        private static final Logger log = LoggerFactory.getLogger(ArtemisSinkWriter.class);

        private final String brokerUrl;
        private final String outQueue;
        private final String user;
        private final String password;
        private final int subtaskId;

        private transient ConnectionFactory connectionFactory;
        private transient Connection connection;
        private transient Session session;
        private transient Destination destination;
        private transient MessageProducer producer;

        ArtemisSinkWriter(
                String brokerUrl,
                String outQueue,
                String user,
                String password,
                int subtaskId
        ) throws IOException {
            this.brokerUrl = brokerUrl;
            this.outQueue = outQueue;
            this.user = user;
            this.password = password;
            this.subtaskId = subtaskId;

            open();
        }

        private void open() throws IOException {
            try {
                log.info(
                        "ArtemisSinkWriter.open() subtask={} brokerUrl={} outQueue={} user={}",
                        subtaskId, brokerUrl, outQueue, user
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
            // без буфера
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
                if (first == null) first = e;
            }

            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                if (first == null) first = e;
            }

            if (connectionFactory instanceof ActiveMQConnectionFactory cf) {
                cf.close();
            }

            if (first != null) {
                throw first;
            }
        }
    }
}
