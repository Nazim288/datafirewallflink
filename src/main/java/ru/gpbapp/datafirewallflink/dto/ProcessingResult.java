package ru.gpbapp.datafirewallflink.dto;

import ru.gpbapp.datafirewallflink.mq.BrokerReply;

public class ProcessingResult {

    private BrokerReply shortReply;
    private String detailJson;

    public ProcessingResult() {
    }

    public ProcessingResult(BrokerReply shortReply, String detailJson) {
        this.shortReply = shortReply;
        this.detailJson = detailJson;
    }

    public BrokerReply getShortReply() {
        return shortReply;
    }

    public void setShortReply(BrokerReply shortReply) {
        this.shortReply = shortReply;
    }

    public String getDetailJson() {
        return detailJson;
    }

    public void setDetailJson(String detailJson) {
        this.detailJson = detailJson;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "shortReply=" + shortReply +
                ", detailJson='" + detailJson + '\'' +
                '}';
    }
}
