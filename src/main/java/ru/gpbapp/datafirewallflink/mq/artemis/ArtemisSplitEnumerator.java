package ru.gpbapp.datafirewallflink.mq.artemis;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.List;

public class ArtemisSplitEnumerator implements SplitEnumerator<ArtemisSplit, Boolean> {

    private final SplitEnumeratorContext<ArtemisSplit> context;
    private boolean assigned;

    public ArtemisSplitEnumerator(SplitEnumeratorContext<ArtemisSplit> context, boolean assigned) {
        this.context = context;
        this.assigned = assigned;
    }

    @Override
    public void start() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!assigned) {
            context.assignSplit(new ArtemisSplit("artemis-split"), subtaskId);
            assigned = true;
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<ArtemisSplit> splits, int subtaskId) {
        if (splits != null && !splits.isEmpty()) {
            assigned = false;
        }
    }

    @Override
    public void addReader(int subtaskId) {
        if (!assigned) {
            context.assignSplit(new ArtemisSplit("artemis-split"), subtaskId);
            assigned = true;
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Boolean snapshotState(long checkpointId) {
        return assigned;
    }

    @Override
    public void close() throws IOException {
    }
}
