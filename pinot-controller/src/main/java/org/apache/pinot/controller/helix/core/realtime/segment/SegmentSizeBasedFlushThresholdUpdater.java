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

import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Updates the flush threshold rows of the new segment metadata, based on the segment size and number of rows of
 * previous segment
 * The formula used to compute new number of rows is:
 * targetNumRows = ideal_segment_size * (a * current_rows_to_size_ratio + b * previous_rows_to_size_ratio)
 * where a = 0.1, b = 0.9, prev ratio= ratio collected over all previous segment completions
 * This ensures that we take into account the history of the segment size and number rows
 */
public class SegmentSizeBasedFlushThresholdUpdater implements FlushThresholdUpdater {
  public static final Logger LOGGER = LoggerFactory.getLogger(SegmentSizeBasedFlushThresholdUpdater.class);
  private final SegmentFlushThresholdComputer _flushThresholdComputer;

  public SegmentSizeBasedFlushThresholdUpdater() {
    _flushThresholdComputer = new SegmentFlushThresholdComputer();
  }

  // synchronized since this method could be called for multiple partitions of the same table in different threads
  @Override
  public synchronized void updateFlushThreshold(StreamConfig streamConfig, SegmentZKMetadata newSegmentZKMetadata,
      CommittingSegmentDescriptor committingSegmentDescriptor, @Nullable SegmentZKMetadata committingSegmentZKMetadata,
      int maxNumPartitionsPerInstance) {
    int threshold =
        _flushThresholdComputer.computeThreshold(streamConfig, committingSegmentDescriptor, committingSegmentZKMetadata,
            newSegmentZKMetadata.getSegmentName());
    newSegmentZKMetadata.setSizeThresholdToFlushSegment(threshold);
  }
}
