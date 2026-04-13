package ru.gpbapp.datafirewallflink.mq.ibm;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IbmMqSplitSerializer implements SimpleVersionedSerializer<IbmMqSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IbmMqSplit split) throws IOException {
        return split.splitId().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public IbmMqSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported IbmMqSplit version: " + version);
        }
        return new IbmMqSplit(new String(serialized, StandardCharsets.UTF_8));
    }
}
