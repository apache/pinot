/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import javax.annotation.Nonnull;


/**
 * Interface for the flush threshold updation strategies
 * These implementations are responsible for updating the flush threshold (rows/time) in the given segment metadata
 */
public interface FlushThresholdUpdater {

  /**
   * Updated the flush threshold of the segment metadata
   * @param newSegmentZKMetadata - new segment metadata for which the thresholds need to be set
   * @param committingSegmentZKMetadata - metadata of the committing segment
   * @param committingSegmentDescriptor
   * @param partitionAssignment - partition assignment for the table
   */
  void updateFlushThreshold(@Nonnull LLCRealtimeSegmentZKMetadata newSegmentZKMetadata,
      LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata, CommittingSegmentDescriptor committingSegmentDescriptor,
      PartitionAssignment partitionAssignment);
}
