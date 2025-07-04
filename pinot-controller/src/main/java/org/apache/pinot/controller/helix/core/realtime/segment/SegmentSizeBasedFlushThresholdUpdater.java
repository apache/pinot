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

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * Updates the flush threshold rows of the new segment metadata, based on the segment size and number of rows of
 * previous segment
 * The formula used to compute new number of rows is:
 * targetNumRows = ideal_segment_size * (a * current_rows_to_size_ratio + b * previous_rows_to_size_ratio)
 * where a = 0.1, b = 0.9, prev ratio= ratio collected over all previous segment completions
 * This ensures that we take into account the history of the segment size and number rows
 */
public class SegmentSizeBasedFlushThresholdUpdater implements FlushThresholdUpdater {
  private final SizeBasedSegmentFlushThresholdComputer _flushThresholdComputer;
  private final String _realtimeTableName;
  private final String _topicName;

  private final ControllerMetrics _controllerMetrics = ControllerMetrics.get();

  public SegmentSizeBasedFlushThresholdUpdater(String realtimeTableName, String topicName) {
    _flushThresholdComputer = new SizeBasedSegmentFlushThresholdComputer();
    _realtimeTableName = realtimeTableName;
    _topicName = topicName;
  }

  @Override
  public void onSegmentCommit(StreamConfig streamConfig, CommittingSegmentDescriptor committingSegmentDescriptor,
      SegmentZKMetadata committingSegmentZKMetadata) {
    long segmentSize = committingSegmentDescriptor.getSegmentSizeBytes();
    _controllerMetrics.setOrUpdateTableGauge(_realtimeTableName, ControllerGauge.COMMITTING_SEGMENT_SIZE, segmentSize);
    _controllerMetrics.setOrUpdateTableGauge(_realtimeTableName, _topicName,
        ControllerGauge.COMMITTING_SEGMENT_SIZE_WITH_TOPIC, segmentSize);

    _flushThresholdComputer.onSegmentCommit(committingSegmentDescriptor, committingSegmentZKMetadata);
  }

  @Override
  public void updateFlushThreshold(StreamConfig streamConfig, SegmentZKMetadata newSegmentZKMetadata,
      int maxNumPartitionsPerInstance) {
    int threshold = _flushThresholdComputer.computeThreshold(streamConfig, newSegmentZKMetadata.getSegmentName());
    newSegmentZKMetadata.setSizeThresholdToFlushSegment(threshold);

    _controllerMetrics.setOrUpdateTableGauge(_realtimeTableName, ControllerGauge.NUM_ROWS_THRESHOLD, threshold);
    _controllerMetrics.setOrUpdateTableGauge(_realtimeTableName, _topicName,
        ControllerGauge.NUM_ROWS_THRESHOLD_WITH_TOPIC, threshold);
  }
}
