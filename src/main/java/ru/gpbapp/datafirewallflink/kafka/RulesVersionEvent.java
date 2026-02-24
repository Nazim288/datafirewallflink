package ru.gpbapp.datafirewallflink.kafka;

import java.io.Serializable;

public class RulesVersionEvent implements Serializable {
    public long version;

    public RulesVersionEvent() {}

    public RulesVersionEvent(long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "RulesVersionEvent{version=" + version + "}";
    }
}

