package ru.gpbapp.datafirewallflink.mq.ibm;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.List;

public class IbmMqSplitEnumerator implements SplitEnumerator<IbmMqSplit, Boolean> {

    private final SplitEnumeratorContext<IbmMqSplit> context;
    private boolean assigned;

    public IbmMqSplitEnumerator(SplitEnumeratorContext<IbmMqSplit> context, boolean assigned) {
        this.context = context;
        this.assigned = assigned;
    }

    @Override
    public void start() {
        // nothing
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!assigned) {
            context.assignSplit(new IbmMqSplit("ibm-mq-split"), subtaskId);
            assigned = true;
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<IbmMqSplit> splits, int subtaskId) {
        if (splits != null && !splits.isEmpty()) {
            assigned = false;
        }
    }

    @Override
    public void addReader(int subtaskId) {
        if (!assigned) {
            context.assignSplit(new IbmMqSplit("ibm-mq-split"), subtaskId);
            assigned = true;
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Boolean snapshotState(long checkpointId) throws Exception {
        return assigned;
    }

    @Override
    public void close() throws IOException {
        // nothing
    }
}
