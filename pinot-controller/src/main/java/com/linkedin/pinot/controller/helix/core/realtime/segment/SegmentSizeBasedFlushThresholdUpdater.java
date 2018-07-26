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

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Updates the flush threshold rows of the new segment metadata, based on the segment size and number of rows of previous segment
 * The formula used to compute new number of rows is:
 * targetNumRows = ideal_segment_size * (a * current_rows_to_size_ratio + b * previous_rows_to_size_ratio)
 * where a = 0.25, b = 0.75, prev ratio= ratio collected over all previous segment completions
 * This ensures that we take into account the history of the segment size and number rows
 */
public class SegmentSizeBasedFlushThresholdUpdater implements FlushThresholdUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentSizeBasedFlushThresholdUpdater.class);

  private static final int INITIAL_ROWS_THRESHOLD = 100_000;

  private static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.1;
  private static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.9;
  private static final double ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT = 1.1;
  private static final int MINIMUM_NUM_ROWS_THRESHOLD = 10_000;

  private final long _desiredSegmentSizeBytes;

  /** Below this size, we double the rows threshold */
  private final double _optimalSegmentSizeBytesMin;
  /** Above this size we half the row threshold */
  private final double _optimalSegmentSizeBytesMax;

  @VisibleForTesting
  int getInitialRowsThreshold() {
    return INITIAL_ROWS_THRESHOLD;
  }

  @VisibleForTesting
  double getRowsMultiplierWhenTimeThresholdHit() {
    return ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT;
  }

  @VisibleForTesting
  int getMinimumNumRowsThreshold() {
    return MINIMUM_NUM_ROWS_THRESHOLD;
  }

  @VisibleForTesting
  long getDesiredSegmentSizeBytes() {
    return _desiredSegmentSizeBytes;
  }

  @VisibleForTesting
  double getLatestSegmentRowsToSizeRatio() {
    return _latestSegmentRowsToSizeRatio;
  }

  // num rows to segment size ratio of last committed segment for this table
  private double _latestSegmentRowsToSizeRatio = 0;

  public SegmentSizeBasedFlushThresholdUpdater(long desiredSegmentSizeBytes) {
    _desiredSegmentSizeBytes = desiredSegmentSizeBytes;
    _optimalSegmentSizeBytesMin = _desiredSegmentSizeBytes / 2;
    _optimalSegmentSizeBytesMax = _desiredSegmentSizeBytes * 1.5;
  }

  // synchronized since this method could be called for multiple partitions of the same table in different threads
  @Override
  public synchronized void updateFlushThreshold(@Nonnull LLCRealtimeSegmentZKMetadata newSegmentZKMetadata,
      LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata,
      @Nonnull CommittingSegmentDescriptor committingSegmentDescriptor, PartitionAssignment partitionAssignment) {

    final String newSegmentName = newSegmentZKMetadata.getSegmentName();
    if (committingSegmentZKMetadata == null) { // first segment of the partition, hence committing segment is null
      if (_latestSegmentRowsToSizeRatio > 0) { // new partition added case
        long targetSegmentNumRows = (long) (_desiredSegmentSizeBytes * _latestSegmentRowsToSizeRatio);
        targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
        LOGGER.info(
            "Committing segment zk metadata is not available, using prev ratio {}, setting rows threshold for {} as {}",
            _latestSegmentRowsToSizeRatio, newSegmentName, targetSegmentNumRows);
        newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
      } else {
        LOGGER.info("Committing segment zk metadata is not available, setting threshold for {} as {}",
            newSegmentName, INITIAL_ROWS_THRESHOLD);
        newSegmentZKMetadata.setSizeThresholdToFlushSegment(INITIAL_ROWS_THRESHOLD);
      }
      return;
    }

    long committingSegmentSizeBytes = committingSegmentDescriptor.getSegmentSizeBytes();
    if (committingSegmentSizeBytes <= 0) { // repair segment case
      final int targetNumRows = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
      LOGGER.info(
          "Committing segment size is not available, setting thresholds from previous segment for {} as {}",
          newSegmentZKMetadata.getSegmentName(), targetNumRows);
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(targetNumRows);
      return;
    }

    long timeConsumed = System.currentTimeMillis() - committingSegmentZKMetadata.getCreationTime();
    long numRowsConsumed = committingSegmentZKMetadata.getTotalRawDocs();
    int numRowsThreshold = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
    LOGGER.info("{}: Data from committing segment: Time {}  numRows {} threshold {} segmentSize(bytes) {}",
        newSegmentName, TimeUtils.convertMillisToPeriod(timeConsumed), numRowsConsumed, numRowsThreshold,
        committingSegmentSizeBytes);

    double currentRatio = (double) numRowsConsumed / committingSegmentSizeBytes;
    // Compute segment size to rows ratio only from partition 0.
    // If we consider all partitions then it is likely that we will assign a much higher weight to the most
    // recent trend in the table (since it is usually true that all partitions of the same table follow more or
    // less same characteristics at any one point in time).
    // However, when we start a new table or change controller mastership, we can have any partition completing first.
    // It is best to learn the ratio as quickly as we can, so we allow any partition to supply the value.
    if (new LLCSegmentName(committingSegmentZKMetadata.getSegmentName()).getPartitionId() == 0 || _latestSegmentRowsToSizeRatio == 0) {
      if (_latestSegmentRowsToSizeRatio > 0) {
        _latestSegmentRowsToSizeRatio =
            CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio + PREVIOUS_SEGMENT_RATIO_WEIGHT * _latestSegmentRowsToSizeRatio;
      } else {
        _latestSegmentRowsToSizeRatio = currentRatio;
      }
    }

    if (numRowsConsumed < numRowsThreshold) {
      // TODO: add feature to adjust time threshold as well
      // If we set new threshold to be numRowsConsumed, we might keep oscillating back and forth between doubling limit
      // and time threshold being hit If we set new threshold to be committingSegmentZKMetadata.getSizeThresholdToFlushSegment(),
      // we might end up using a lot more memory than required for the segment Using a minor bump strategy, until
      // we add feature to adjust time We will only slightly bump the threshold based on numRowsConsumed
      long targetSegmentNumRows = (long) (numRowsConsumed * ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT);
      targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
      LOGGER.info("Time threshold reached, setting segment size for {} as {}", newSegmentName, targetSegmentNumRows);
      newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
      return;
    }

    long targetSegmentNumRows;
    if (committingSegmentSizeBytes < _optimalSegmentSizeBytesMin) {
      targetSegmentNumRows = numRowsConsumed + numRowsConsumed / 2;
    } else if (committingSegmentSizeBytes > _optimalSegmentSizeBytesMax) {
      targetSegmentNumRows = numRowsConsumed / 2;
    } else {
      if (_latestSegmentRowsToSizeRatio > 0) {
        targetSegmentNumRows = (long) (_desiredSegmentSizeBytes * _latestSegmentRowsToSizeRatio);
      } else {
        targetSegmentNumRows = (long) (_desiredSegmentSizeBytes * currentRatio);
      }
    }
    targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
    LOGGER.info("Committing segment size {}, current ratio {}, setting threshold for {} as {}",
      committingSegmentSizeBytes, _latestSegmentRowsToSizeRatio, newSegmentName, targetSegmentNumRows);

    newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
  }

  private long capNumRowsIfOverflow(long targetSegmentNumRows) {
    if (targetSegmentNumRows > Integer.MAX_VALUE) {
      // TODO Picking Integer.MAX_VALUE for number of rows will most certainly make the segment unloadable
      // so we need to pick a lower value here. But before that, we need to consider why the value may
      // go so high and prevent it. We will definitely reach a high segment size long before we get here...
      targetSegmentNumRows = Integer.MAX_VALUE;
    }
    return Math.max(targetSegmentNumRows, MINIMUM_NUM_ROWS_THRESHOLD);
  }
}
