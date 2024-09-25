package org.apache.pinot.controller.helix.core.realtime;

import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public interface SegmentCompletionFSM {
  boolean isDone();
  SegmentCompletionProtocol.Response segmentConsumed(String instanceId, StreamPartitionMsgOffset offset, String stopReason);
  SegmentCompletionProtocol.Response segmentCommitStart(String instanceId, StreamPartitionMsgOffset offset);
  SegmentCompletionProtocol.Response stoppedConsuming(String instanceId, StreamPartitionMsgOffset offset, String reason);
  SegmentCompletionProtocol.Response extendBuildTime(String instanceId, StreamPartitionMsgOffset offset, int extTimeSec);
  SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams, boolean success, boolean isSplitCommit, CommittingSegmentDescriptor committingSegmentDescriptor);
}

