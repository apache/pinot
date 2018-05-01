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

import com.google.common.util.concurrent.AtomicDouble;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Updates the flush threshold rows of the new segment metadata, based on the segment size and number of rows of previous segment
 * The formula used to compute new number of rows is:
 * targetNumRows = ideal_segment_size * ( a * current_rows_to_size_ratio + b * previous_rows_to_size_ratio)
 * where a = 0.25, b = 0.75
 * This ensures that we take into account the history of the segment size and number rows
 */
public class SegmentSizeBasedFlushThresholdUpdater extends FlushThresholdUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentSizeBasedFlushThresholdUpdater.class);

  static final long IDEAL_SEGMENT_SIZE_BYTES = 500 * 1024 * 1024;
  static final double MIN_ALLOWED_SEGMENT_SIZE_BYTES = 250 * 1024 * 1024;
  static final double MAX_ALLOWED_SEGMENT_SIZE_BYTES = 750 * 1024 * 1024;
  static final int INITIAL_ROWS_THRESHOLD = 100_000;
  static final String MAX_TIME_THRESHOLD = "24h";

  private static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.25;
  private static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.75;

  private AtomicDouble _latestSegmentRowsToSizeRatio = new AtomicDouble();

  protected SegmentSizeBasedFlushThresholdUpdater(TableConfig realtimeTableConfig) {
    super(realtimeTableConfig);
  }

  @Override
  public void updateFlushThreshold(@Nonnull LLCRealtimeSegmentZKMetadata newSegmentZKMetadata,
      @Nonnull FlushThresholdUpdaterParams params) {

    LLCRealtimeSegmentZKMetadata committingSegmentZkMetadata = params.getCommittingSegmentZkMetadata();
    if (committingSegmentZkMetadata == null) { // first segment of the partition
      double prevRatio = _latestSegmentRowsToSizeRatio.get();
      if (prevRatio > 0) {
        LOGGER.info(
            "Committing segment zk metadata is not available, setting rows threshold for segment using previous segments ratio {}",
            newSegmentZKMetadata.getSegmentName());
        newSegmentZKMetadata.setSizeThresholdToFlushSegment((int) (IDEAL_SEGMENT_SIZE_BYTES * prevRatio));
      } else {
        LOGGER.info("Committing segment zk metadata is not available, setting default rows threshold for segment {}",
            newSegmentZKMetadata.getSegmentName());
        newSegmentZKMetadata.setSizeThresholdToFlushSegment(INITIAL_ROWS_THRESHOLD);
      }
      newSegmentZKMetadata.setTimeThresholdToFlushSegment(MAX_TIME_THRESHOLD);
      return;
    }

    long committingSegmentSizeBytes = params.getCommittingSegmentSizeBytes();
    if (committingSegmentSizeBytes <= 0) { // repair segment
      LOGGER.info(
          "Committing segment size is not available, setting thresholds for segment {} from previous segment {}",
          newSegmentZKMetadata.getSegmentName(), committingSegmentZkMetadata.getSegmentName());
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(committingSegmentZkMetadata.getSizeThresholdToFlushSegment());
      newSegmentZKMetadata.setTimeThresholdToFlushSegment(committingSegmentZkMetadata.getTimeThresholdToFlushSegment());
    }

    // time taken by committing segment
    long timeConsumed = committingSegmentZkMetadata.getEndTime() - committingSegmentZkMetadata.getStartTime();

    // rows consumed by committing segment
    int numRowsConsumed =
        (int) (committingSegmentZkMetadata.getEndOffset() - committingSegmentZkMetadata.getStartOffset());

    // rows threshold for committing segment
    int numRowsThreshold = committingSegmentZkMetadata.getSizeThresholdToFlushSegment();

    if (timeConsumed >= TimeUtils.convertPeriodToMillis(MAX_TIME_THRESHOLD)) {
      LOGGER.info("Time threshold {} reached for committing segment {}", MAX_TIME_THRESHOLD,
          committingSegmentZkMetadata.getSegmentName());
    } else if (numRowsConsumed >= numRowsThreshold) {
      LOGGER.info("Number of rows threshold {} reached for segment {}", numRowsThreshold,
          committingSegmentZkMetadata.getSegmentName());
    }

    double currentRatio = (double) numRowsConsumed / committingSegmentSizeBytes;
    double prevRatio = _latestSegmentRowsToSizeRatio.getAndSet(currentRatio);
    if (committingSegmentSizeBytes < MIN_ALLOWED_SEGMENT_SIZE_BYTES) {
      // double the number of rows
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(numRowsConsumed * 2);
      newSegmentZKMetadata.setTimeThresholdToFlushSegment(MAX_TIME_THRESHOLD);
    } else if (committingSegmentSizeBytes > MAX_ALLOWED_SEGMENT_SIZE_BYTES) {
      // half it
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(numRowsConsumed / 2);
      newSegmentZKMetadata.setTimeThresholdToFlushSegment(MAX_TIME_THRESHOLD);
    } else {
      int targetSegmentNumRows;
      if (prevRatio <= 0) {
        targetSegmentNumRows = (int) (IDEAL_SEGMENT_SIZE_BYTES * currentRatio);
      } else {
        targetSegmentNumRows = (int) (IDEAL_SEGMENT_SIZE_BYTES * (CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio
            + PREVIOUS_SEGMENT_RATIO_WEIGHT * prevRatio));
      }
      newSegmentZKMetadata.setSizeThresholdToFlushSegment(targetSegmentNumRows);
      newSegmentZKMetadata.setTimeThresholdToFlushSegment(MAX_TIME_THRESHOLD);
    }
  }
}
