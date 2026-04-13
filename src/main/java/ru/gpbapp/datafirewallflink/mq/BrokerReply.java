package ru.gpbapp.datafirewallflink.mq;

import java.io.Serializable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class BrokerReply implements Serializable {
    public static final int IBM_MQ_ID_LEN = 24;

    public String correlationId;
    public byte[] correlationIdBytes;
    public String payload;

    public BrokerReply() {
    }

    public BrokerReply(String correlationId, String payload) {
        this.correlationId = correlationId;
        this.correlationIdBytes = toIbmMqBytes(correlationId);
        this.payload = payload;
    }

    public BrokerReply(byte[] correlationIdBytes, String payload) {
        this.correlationIdBytes = normalizeId(correlationIdBytes);
        this.correlationId = fromIbmMqBytes(this.correlationIdBytes);
        this.payload = payload;
    }

    public static byte[] normalizeId(byte[] id) {
        if (id == null) {
            return null;
        }
        if (id.length == IBM_MQ_ID_LEN) {
            return Arrays.copyOf(id, IBM_MQ_ID_LEN);
        }

        byte[] out = new byte[IBM_MQ_ID_LEN];
        System.arraycopy(id, 0, out, 0, Math.min(id.length, IBM_MQ_ID_LEN));
        return out;
    }

    public static byte[] toIbmMqBytes(String id) {
        if (id == null) {
            return null;
        }
        return normalizeId(id.getBytes(StandardCharsets.UTF_8));
    }

    public static String fromIbmMqBytes(byte[] id) {
        if (id == null) {
            return null;
        }

        int len = id.length;
        while (len > 0 && id[len - 1] == 0) {
            len--;
        }
        return new String(id, 0, len, StandardCharsets.UTF_8);
    }
}
