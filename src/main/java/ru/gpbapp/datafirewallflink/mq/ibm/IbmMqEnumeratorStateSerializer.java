package ru.gpbapp.datafirewallflink.mq.ibm;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class IbmMqEnumeratorStateSerializer implements SimpleVersionedSerializer<Boolean> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Boolean assigned) throws IOException {
        return new byte[] { (byte) (Boolean.TRUE.equals(assigned) ? 1 : 0) };
    }

    @Override
    public Boolean deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported enumerator state version: " + version);
        }
        return serialized != null && serialized.length > 0 && serialized[0] == 1;
    }
}
