/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.realtime.segment;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * The default flush threshold updation strategy, which computes the flush threshold size of the segment
 * by dividing the flush threshold of the table by the max number of partitions consuming on an instance
 */
public class DefaultFlushThresholdUpdater implements FlushThresholdUpdater {
  private final int _tableFlushSize;

  DefaultFlushThresholdUpdater(int tableFlushSize) {
    _tableFlushSize = tableFlushSize;
  }

  @Override
  public void updateFlushThreshold(StreamConfig streamConfig, SegmentZKMetadata newSegmentZKMetadata,
      CommittingSegmentDescriptor committingSegmentDescriptor, @Nullable SegmentZKMetadata committingSegmentZKMetadata,
      int maxNumPartitionsPerInstance, List<PartitionGroupMetadata> partitionGroupMetadataList) {
    // Configure the segment size flush limit based on the maximum number of partitions allocated to an instance
    newSegmentZKMetadata.setSizeThresholdToFlushSegment(_tableFlushSize / maxNumPartitionsPerInstance);
  }

  @VisibleForTesting
  int getTableFlushSize() {
    return _tableFlushSize;
  }
}
