package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class ArtemisSplit implements SourceSplit, Serializable {

    private final String splitId;

    public ArtemisSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
