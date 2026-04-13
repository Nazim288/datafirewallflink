package ru.gpbapp.datafirewallflink.mq.ibm;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class IbmMqSplit implements SourceSplit, Serializable {

    private final String splitId;

    public IbmMqSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
