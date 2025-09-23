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
import java.time.Clock;
import java.util.Random;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
class SizeBasedSegmentFlushThresholdComputer {
  static final int MINIMUM_NUM_ROWS_THRESHOLD = 10_000;
  static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.1;
  static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.9;
  static final double ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT = 1.1;

  private static final Logger LOGGER = LoggerFactory.getLogger(SizeBasedSegmentFlushThresholdComputer.class);
  private static final Random RANDOM = new Random();

  private final Clock _clock;

  private long _timeConsumedForLastSegment;
  private int _rowsConsumedForLastSegment;
  private long _sizeForLastSegment;
  private int _rowsThresholdForLastSegment;
  private double _segmentRowsToSizeRatio;

  SizeBasedSegmentFlushThresholdComputer() {
    this(Clock.systemUTC());
  }

  @VisibleForTesting
  SizeBasedSegmentFlushThresholdComputer(Clock clock) {
    _clock = clock;
  }

  @VisibleForTesting
  public void setSizeForLastSegment(long sizeForLastSegment) {
    _sizeForLastSegment = sizeForLastSegment;
  }

  @VisibleForTesting
  void setSegmentRowsToSizeRatio(double segmentRowsToSizeRatio) {
    _segmentRowsToSizeRatio = segmentRowsToSizeRatio;
  }

  @VisibleForTesting
  double getSegmentRowsToSizeRatio() {
    return _segmentRowsToSizeRatio;
  }

  synchronized void onSegmentCommit(CommittingSegmentDescriptor committingSegmentDescriptor,
      SegmentZKMetadata committingSegmentZKMetadata) {
    String segmentName = committingSegmentZKMetadata.getSegmentName();
    long postCommitRows = committingSegmentZKMetadata.getTotalDocs();
    long preCommitRows = committingSegmentDescriptor.getPreCommitRowCount();
    long postCommitSizeBytes = committingSegmentDescriptor.getSegmentSizeBytes();

    // Use pre-commit rows if available (for commit time compaction), otherwise use post-commit rows
    boolean usingPreCommitRows = preCommitRows > 0 && preCommitRows > postCommitRows;
    long rowsForCalculation = usingPreCommitRows ? preCommitRows : postCommitRows;

    // Estimate pre-commit size when using pre-commit rows. If post-commit rows are 0 (cannot infer),
    // fall back to using post-commit size for estimation.
    long sizeForCalculation =
        calculateSizeForCalculation(usingPreCommitRows, preCommitRows, postCommitRows, postCommitSizeBytes);

    // Skip updating the ratio if the segment is empty, size is not available, or the segment is force-committed.
    if (rowsForCalculation <= 0 || sizeForCalculation <= 0
        || SegmentCompletionProtocol.REASON_FORCE_COMMIT_MESSAGE_RECEIVED.equals(
        committingSegmentDescriptor.getStopReason())) {
      if (committingSegmentZKMetadata.getStatus() == Status.DONE) {
        // Do not log for COMMITTING segment, as it is expected to not have rowsConsumed and sizeInBytes set
        LOGGER.info(
            "Skipping updating segment rows to size ratio for segment: {} with rows: {}, size: {} and stop reason: {}",
            segmentName, rowsForCalculation, sizeForCalculation, committingSegmentDescriptor.getStopReason());
      }
      // When segment rows to size ratio is not available, update the rows threshold to be used for the next segment.
      // For pauseless consumption, this can ensure the first new consuming segment carries over the rows threshold from
      // the previous segment,
      if (_segmentRowsToSizeRatio == 0) {
        int rowsThreshold = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
        LOGGER.info("Segment rows to size ratio is not available, updating rows threshold to: {}", rowsThreshold);
        _rowsThresholdForLastSegment = rowsThreshold;
      }
      return;
    }

    long timeConsumed = _clock.millis() - committingSegmentZKMetadata.getCreationTime();
    int rowsThreshold = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();

    // Store values using the actual rows consumed for threshold calculations
    _timeConsumedForLastSegment = timeConsumed;
    _rowsConsumedForLastSegment = (int) rowsForCalculation;
    _sizeForLastSegment = sizeForCalculation;
    _rowsThresholdForLastSegment = rowsThreshold;

    // Calculate ratio using actual rows (pre-commit if available)
    double segmentRatio = (double) rowsForCalculation / (double) sizeForCalculation;
    double currentRatio = _segmentRowsToSizeRatio;

      // Update the segment rows to size ratio using weighted average
    if (currentRatio > 0) {
      _segmentRowsToSizeRatio =
          CURRENT_SEGMENT_RATIO_WEIGHT * segmentRatio + PREVIOUS_SEGMENT_RATIO_WEIGHT * currentRatio;
    } else {
      _segmentRowsToSizeRatio = segmentRatio;
    }
    LOGGER.info(
        "Updated segment: {}, time: {}, rows used for calculation: {} (pre-commit: {}, post-commit: {}), size used: {}"
            + " (post-commit size: {}), consuming ratio: {}, threshold: {}."
            + " Segment rows to size ratio updated from: {} to: {}",
        segmentName, TimeUtils.convertMillisToPeriod(timeConsumed), rowsForCalculation, preCommitRows, postCommitRows,
        sizeForCalculation, postCommitSizeBytes, segmentRatio, rowsThreshold, currentRatio, _segmentRowsToSizeRatio);
  }

  synchronized int computeThreshold(StreamConfig streamConfig, String segmentName) {
    if (_segmentRowsToSizeRatio == 0) {
      if (_rowsThresholdForLastSegment > 0) {
        LOGGER.info("Segment rows to size ratio is not available, using rows threshold: {} from previous segment for "
            + "new segment: {}", _rowsThresholdForLastSegment, segmentName);
        return _rowsThresholdForLastSegment;
      } else {
        int initialRows = streamConfig.getFlushAutotuneInitialRows();
        LOGGER.info("This is the first segment, using initial rows threshold: {} for segment: {}", initialRows,
            segmentName);
        return initialRows;
      }
    }

    long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    if (desiredSegmentSizeBytes <= 0) {
      desiredSegmentSizeBytes = StreamConfig.DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES;
    }

    // If the number of rows consumed is less than what we set as target in metadata, then the segment hit time limit.
    // We can set the new target to be slightly higher than the actual number of rows consumed so that we can aim
    // to hit the row limit next time around.
    //
    // If the size of the committing segment is higher than the desired segment size, then the administrator has
    // set a lower segment size threshold. We should treat this case as if we have hit thw row limit and not the time
    // limit.
    //
    // TODO: add feature to adjust time threshold as well
    // If we set new threshold to be numRowsConsumed, we might keep oscillating back and forth between doubling limit
    // and time threshold being hit If we set new threshold to be committingSegmentZKMetadata
    // .getSizeThresholdToFlushSegment(),
    // we might end up using a lot more memory than required for the segment Using a minor bump strategy, until
    // we add feature to adjust time We will only slightly bump the threshold based on numRowsConsumed
    if (_rowsConsumedForLastSegment < _rowsThresholdForLastSegment && _sizeForLastSegment < desiredSegmentSizeBytes) {
      long timeThresholdMs = streamConfig.getFlushThresholdTimeMillis();
      long rowsConsumed = _rowsConsumedForLastSegment;
      StringBuilder logStringBuilder = new StringBuilder().append("Time threshold reached. ");
      if (timeThresholdMs < _timeConsumedForLastSegment) {
        // The administrator has reduced the time threshold. Adjust the
        // number of rows to match the average consumption rate on the partition.
        rowsConsumed = timeThresholdMs * rowsConsumed / _timeConsumedForLastSegment;
        logStringBuilder.append("Detected lower time threshold, adjusting numRowsConsumed to: ").append(rowsConsumed)
            .append(". ");
      }
      int threshold = getThreshold((long) (rowsConsumed * ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT));
      logStringBuilder.append("Setting segment size threshold for: ")
          .append(segmentName)
          .append(" to: ")
          .append(threshold);
      LOGGER.info(logStringBuilder.toString());
      return threshold;
    }

    long optimalSegmentSizeBytesMin = desiredSegmentSizeBytes / 2;
    double optimalSegmentSizeBytesMax = desiredSegmentSizeBytes * 1.5;
    long targetRows;
    if (_sizeForLastSegment < optimalSegmentSizeBytesMin) {
      targetRows = (long) (_rowsConsumedForLastSegment * 1.5);
    } else if (_sizeForLastSegment > optimalSegmentSizeBytesMax) {
      targetRows = _rowsConsumedForLastSegment / 2;
    } else {
      targetRows = (long) (desiredSegmentSizeBytes * _segmentRowsToSizeRatio);
    }
    double variance = streamConfig.getFlushThresholdVarianceFraction();
    if (variance > 0) {
      LOGGER.info("Applying variance: {} to segment: {} with target rows: {}", variance, segmentName, targetRows);
      double variation = (1 - variance) + 2 * variance * RANDOM.nextDouble();
      targetRows = (long) (targetRows * variation);
    }
    int threshold = getThreshold(targetRows);
    LOGGER.info("Setting segment size threshold for: {} to: {}", segmentName, threshold);
    return threshold;
  }

  private int getThreshold(long targetRows) {
    if (targetRows > Integer.MAX_VALUE) {
      // TODO Picking Integer.MAX_VALUE for number of rows will most certainly make the segment unloadable
      // so we need to pick a lower value here. But before that, we need to consider why the value may
      // go so high and prevent it. We will definitely reach a high segment size long before we get here...
      return Integer.MAX_VALUE;
    }
    return Math.max((int) targetRows, MINIMUM_NUM_ROWS_THRESHOLD);
  }

  /**
   * Calculates the size to use for segment size ratio calculations.
   * When using pre-commit rows, attempts to estimate the pre-commit size based on the compression ratio
   * observed between pre-commit and post-commit rows. Falls back to post-commit size if estimation
   * is not possible.
   *
   * @param usingPreCommitRows whether pre-commit rows are being used for calculation
   * @param preCommitRows the number of rows before commit-time compaction
   * @param postCommitRows the number of rows after commit-time compaction
   * @param postCommitSizeBytes the actual segment size after commit-time compaction
   * @return the estimated size to use for ratio calculations
   */
  @VisibleForTesting
  long calculateSizeForCalculation(
      boolean usingPreCommitRows, long preCommitRows, long postCommitRows, long postCommitSizeBytes) {
    if (usingPreCommitRows) {
      if (postCommitRows > 0) {
        double estimatedPreCommitSize = ((double) postCommitSizeBytes) * ((double) preCommitRows)
            / (double) postCommitRows;
        return Math.round(estimatedPreCommitSize);
      } else {
        return postCommitSizeBytes;
      }
    } else {
      return postCommitSizeBytes;
    }
  }
}
