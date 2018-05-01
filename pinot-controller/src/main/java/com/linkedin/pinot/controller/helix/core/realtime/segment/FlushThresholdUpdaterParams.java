/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.realtime.segment;

import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.partition.PartitionAssignment;


/**
 * Class to hold params required by flush threshold updater strategies
 */
public class FlushThresholdUpdaterParams {
  private LLCRealtimeSegmentZKMetadata _committingSegmentZkMetadata;
  private long _committingSegmentSizeBytes;
  private PartitionAssignment _partitionAssignment;

  public LLCRealtimeSegmentZKMetadata getCommittingSegmentZkMetadata() {
    return _committingSegmentZkMetadata;
  }

  public void setCommittingSegmentZkMetadata(LLCRealtimeSegmentZKMetadata committingSegmentZkMetadata) {
    _committingSegmentZkMetadata = committingSegmentZkMetadata;
  }

  public long getCommittingSegmentSizeBytes() {
    return _committingSegmentSizeBytes;
  }

  public void setCommittingSegmentSizeBytes(long segmentSize) {
    _committingSegmentSizeBytes = segmentSize;
  }

  public PartitionAssignment getPartitionAssignment() {
    return _partitionAssignment;
  }

  public void setPartitionAssignment(PartitionAssignment partitionAssignment) {
    _partitionAssignment = partitionAssignment;
  }
}
