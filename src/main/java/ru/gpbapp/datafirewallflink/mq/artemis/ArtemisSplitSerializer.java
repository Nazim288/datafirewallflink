package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ArtemisSplitSerializer implements SimpleVersionedSerializer<ArtemisSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ArtemisSplit split) throws IOException {
        return split.splitId().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ArtemisSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported ArtemisSplit version: " + version);
        }
        return new ArtemisSplit(new String(serialized, StandardCharsets.UTF_8));
    }
}
