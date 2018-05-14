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

  private static final long IDEAL_SEGMENT_SIZE_BYTES = 200 * 1024 * 1024;
  /** Below this size, we double the rows threshold */
  private static final double OPTIMAL_SEGMENT_SIZE_BYTES_MIN = IDEAL_SEGMENT_SIZE_BYTES / 2;
  /** Above this size we half the row threshold */
  private static final double OPTIMAL_SEGMENT_SIZE_BYTES_MAX = IDEAL_SEGMENT_SIZE_BYTES * 1.5;
  private static final int INITIAL_ROWS_THRESHOLD = 100_000;

  private static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.1;
  private static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.9;
  private static final double ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT = 1.1;

  @VisibleForTesting
  long getIdealSegmentSizeBytes() {
    return IDEAL_SEGMENT_SIZE_BYTES;
  }

  @VisibleForTesting
  int getInitialRowsThreshold() {
    return INITIAL_ROWS_THRESHOLD;
  }

  @VisibleForTesting
  double getRowsMultiplierWhenTimeThresholdHit() {
    return ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT;
  }

  // num rows to segment size ratio of last committed segment for this table
  private double _latestSegmentRowsToSizeRatio = 0;

  @Override
  public void updateFlushThreshold(@Nonnull LLCRealtimeSegmentZKMetadata newSegmentZKMetadata,
      LLCRealtimeSegmentZKMetadata committingSegmentZKMetadata,
      @Nonnull CommittingSegmentDescriptor committingSegmentDescriptor, PartitionAssignment partitionAssignment) {

    if (committingSegmentZKMetadata == null) { // first segment of the partition, hence committing segment is null
      if (_latestSegmentRowsToSizeRatio > 0) { // new partition added case
        LOGGER.info(
            "Committing segment zk metadata is not available, setting rows threshold for segment {} using previous segments ratio",
            newSegmentZKMetadata.getSegmentName());
        long targetSegmentNumRows = (long) (IDEAL_SEGMENT_SIZE_BYTES * _latestSegmentRowsToSizeRatio);
        targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
        newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
      } else {
        LOGGER.info("Committing segment zk metadata is not available, setting default rows threshold for segment {}",
            newSegmentZKMetadata.getSegmentName());
        newSegmentZKMetadata.setSizeThresholdToFlushSegment(INITIAL_ROWS_THRESHOLD);
      }
      return;
    }

    long committingSegmentSizeBytes = committingSegmentDescriptor.getSegmentSizeBytes();
    if (committingSegmentSizeBytes <= 0) { // repair segment case
      LOGGER.info(
          "Committing segment size is not available, setting thresholds for segment {} from previous segment {}",
          newSegmentZKMetadata.getSegmentName(), committingSegmentZKMetadata.getSegmentName());
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(committingSegmentZKMetadata.getSizeThresholdToFlushSegment());
      return;
    }

    long timeConsumed = System.currentTimeMillis() - committingSegmentZKMetadata.getCreationTime();
    long numRowsConsumed = committingSegmentZKMetadata.getTotalRawDocs();
    int numRowsThreshold = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
    LOGGER.info("Time consumed:{}  Num rows consumed:{} Num rows threshold:{} Committing segment size bytes:{}",
        TimeUtils.convertMillisToPeriod(timeConsumed), numRowsConsumed, numRowsThreshold, committingSegmentSizeBytes);

    double currentRatio = (double) numRowsConsumed / committingSegmentSizeBytes;
    if (new LLCSegmentName(committingSegmentZKMetadata.getSegmentName()).getPartitionId() == 0) {
      if (_latestSegmentRowsToSizeRatio > 0) {
        _latestSegmentRowsToSizeRatio =
            CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio + PREVIOUS_SEGMENT_RATIO_WEIGHT * _latestSegmentRowsToSizeRatio;
      } else {
        _latestSegmentRowsToSizeRatio = currentRatio;
      }
    }

    if (numRowsConsumed < numRowsThreshold) {
      // TODO: add feature to adjust time threshold as well
      // If we set new threshold to be numRowsConsumed, we might keep oscillating back and forth between doubling limit and time threshold being hit
      // If we set new threshold to be committingSegmentZKMetadata.getSizeThresholdToFlushSegment(), we might end up using a lot more memory than required for the segment
      // Using a minor bump strategy, until we add feature to adjust time
      // We will only slightly bump the threshold based on numRowsConsumed
      long targetSegmentNumRows = (long) (numRowsConsumed * 1.1);
      targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
      LOGGER.info("Segment {} reached time threshold, bumping segment size threshold 1.1 times to {} for segment {}",
          committingSegmentZKMetadata.getSegmentName(), targetSegmentNumRows, newSegmentZKMetadata.getSegmentName());
      newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
      return;
    }

    long targetSegmentNumRows;
    if (committingSegmentSizeBytes < OPTIMAL_SEGMENT_SIZE_BYTES_MIN) {
      targetSegmentNumRows = numRowsConsumed + numRowsConsumed / 2;
      LOGGER.info("Committing segment size is less than min segment size {}, doubling rows threshold to : {}",
          OPTIMAL_SEGMENT_SIZE_BYTES_MIN, newSegmentZKMetadata.getSizeThresholdToFlushSegment());
    } else if (committingSegmentSizeBytes > OPTIMAL_SEGMENT_SIZE_BYTES_MAX) {
      targetSegmentNumRows = numRowsConsumed / 2;
      LOGGER.info("Committing segment size is greater than max segment size {}, halving rows threshold {}",
          OPTIMAL_SEGMENT_SIZE_BYTES_MAX, newSegmentZKMetadata.getSizeThresholdToFlushSegment());
    } else {
      if (_latestSegmentRowsToSizeRatio > 0) {
        targetSegmentNumRows = (long) (IDEAL_SEGMENT_SIZE_BYTES * _latestSegmentRowsToSizeRatio);
      } else {
        targetSegmentNumRows = (long) (IDEAL_SEGMENT_SIZE_BYTES * currentRatio);
      }
      LOGGER.info("Setting new rows threshold : {}", newSegmentZKMetadata.getSizeThresholdToFlushSegment());
    }
    targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);

    newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) targetSegmentNumRows);
  }

  private long capNumRowsIfOverflow(long targetSegmentNumRows) {
    if (targetSegmentNumRows > Integer.MAX_VALUE) {
      targetSegmentNumRows = Integer.MAX_VALUE;
    }
    return targetSegmentNumRows;
  }
}
