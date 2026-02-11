package ru.gpbapp.datafirewallflink.mq;

import java.io.Serializable;
import java.util.Arrays;

public final class MqReply implements Serializable {
    public static final int MQ_ID_LEN = 24;

    public byte[] correlId;
    public String payload;

    public MqReply() {}

    public MqReply(byte[] correlId, String payload) {
        this.correlId = normalizeId(correlId);
        this.payload = payload;
    }

    private static byte[] normalizeId(byte[] id) {
        if (id == null) return null;
        if (id.length == MQ_ID_LEN) return Arrays.copyOf(id, MQ_ID_LEN);

        // pad/cut to 24 bytes
        byte[] out = new byte[MQ_ID_LEN];
        System.arraycopy(id, 0, out, 0, Math.min(id.length, MQ_ID_LEN));
        return out;
    }
}
