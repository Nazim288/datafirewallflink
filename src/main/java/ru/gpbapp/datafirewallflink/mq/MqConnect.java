package ru.gpbapp.datafirewallflink.mq;

import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

public final class MqConnect {

    private static final Logger log = LoggerFactory.getLogger(MqConnect.class);

    private MqConnect() {}

    public static MQQueueManager connect(String qmgr, String host, int port, String channel) throws Exception {
        return connect(qmgr, host, port, channel, null, null);
    }

    public static MQQueueManager connect(String qmgr,
                                         String host,
                                         int port,
                                         String channel,
                                         String user,
                                         String password) throws Exception {
        Hashtable<String, Object> props = new Hashtable<>();
        props.put(MQConstants.HOST_NAME_PROPERTY, host);
        props.put(MQConstants.PORT_PROPERTY, port);
        props.put(MQConstants.CHANNEL_PROPERTY, channel);
        props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);

        if (user != null && !user.isBlank()) {
            props.put(MQConstants.USER_ID_PROPERTY, user);
            if (password != null) {
                props.put(MQConstants.PASSWORD_PROPERTY, password);
                props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, true);
            }
        }

        log.info("Connecting to IBM MQ qmgr={} {}:{} channel={} auth={}",
                qmgr, host, port, channel, (user != null && !user.isBlank()));

        return new MQQueueManager(qmgr, props);
    }
}
