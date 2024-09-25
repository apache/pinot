package org.apache.pinot.controller.helix.core.realtime;

public enum SegmentCompletionFSMState {
    PARTIAL_CONSUMING,  // Indicates that at least one replica has reported that it has stopped consuming.
    HOLDING,          // the segment has started finalizing.
    COMMITTER_DECIDED, // We know who the committer will be, we will let them know next time they call segmentConsumed()
    COMMITTER_NOTIFIED, // we notified the committer to commit.
    COMMITTER_UPLOADING,  // committer is uploading.
    COMMITTING, // we are in the process of committing to zk
    COMMITTED,    // We already committed a segment.
    ABORTED,      // state machine is aborted. we will start a fresh one when the next segmentConsumed comes in.
}
