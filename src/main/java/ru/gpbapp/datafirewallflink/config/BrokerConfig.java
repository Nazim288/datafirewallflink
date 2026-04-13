package ru.gpbapp.datafirewallflink.config;

import org.apache.flink.api.java.utils.ParameterTool;

public record BrokerConfig (
        String type,          // ibm | artemis
        String host,
        int port,
        String channel,       // only IBM
        String qmgr,          // only IBM
        String brokerUrl,     // mainly Artemis
        String inQueue,
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
    public static BrokerConfig fromArgs(ParameterTool pt) {
        String type = pt.get("mq.type", "ibm");

        String host = pt.get("mq.host", "localhost");
        int port = pt.getInt("mq.port", "artemis".equalsIgnoreCase(type) ? 61616 : 1414);
        String channel = pt.get("mq.channel", "");
        String qmgr = pt.get("mq.qmgr", "");
        String brokerUrl = pt.get("mq.url", "tcp://" + host + ":" + port);
        String inQueue = pt.get("mq.inQueue", "TEST.QUEUE");
        String outQueue = pt.get("mq.outQueue", "REPLY.QUEUE");
        String user = pt.get("mq.user", "");
        String password = pt.get("mq.password", "");
        boolean tlsEnabled = pt.getBoolean("mq.tlsEnabled", false);
        String trustStorePath = pt.get("mq.trustStorePath", "");
        String trustStorePassword = pt.get("mq.trustStorePassword", "");
        String keyStorePath = pt.get("mq.keyStorePath", "");
        String keyStorePassword = pt.get("mq.keyStorePassword", "");
        String cipherSuite = pt.get("mq.cipherSuite", "");

        return new BrokerConfig(type, host, port, channel, qmgr, brokerUrl, inQueue, outQueue, user, password,
                tlsEnabled, trustStorePath, trustStorePassword, keyStorePath, keyStorePassword, cipherSuite
        );
    }
}
