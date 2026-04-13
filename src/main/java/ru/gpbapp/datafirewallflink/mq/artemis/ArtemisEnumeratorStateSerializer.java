package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class ArtemisEnumeratorStateSerializer implements SimpleVersionedSerializer<Boolean> {

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
            throw new IOException("Unsupported Artemis enumerator state version: " + version);
        }
        return serialized != null && serialized.length > 0 && serialized[0] == 1;
    }
}
