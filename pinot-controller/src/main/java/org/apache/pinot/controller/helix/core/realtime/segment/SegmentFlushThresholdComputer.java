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
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.TimeUtils;

import static org.apache.pinot.common.protocols.SegmentCompletionProtocol.REASON_FORCE_COMMIT_MESSAGE_RECEIVED;


class SegmentFlushThresholdComputer {
  public static final int MINIMUM_NUM_ROWS_THRESHOLD = 10_000;
  static final double CURRENT_SEGMENT_RATIO_WEIGHT = 0.1;
  static final double PREVIOUS_SEGMENT_RATIO_WEIGHT = 0.9;
  static final double ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT = 1.1;

  // num rows to segment size ratio of last committed segment for this table
  private double _latestSegmentRowsToSizeRatio;
  private final Clock _clock;

  SegmentFlushThresholdComputer() {
    this(Clock.systemUTC(), 0);
  }

  @VisibleForTesting
  SegmentFlushThresholdComputer(Clock clock, double latestSegmentRowsToSizeRatio) {
    _clock = clock;
    _latestSegmentRowsToSizeRatio = latestSegmentRowsToSizeRatio;
  }

  @VisibleForTesting
  SegmentFlushThresholdComputer(Clock clock) {
    this(clock, 0);
  }

  double getLatestSegmentRowsToSizeRatio() {
    return _latestSegmentRowsToSizeRatio;
  }

  public int computeThreshold(StreamConfig streamConfig, CommittingSegmentDescriptor committingSegmentDescriptor,
      @Nullable SegmentZKMetadata committingSegmentZKMetadata, String newSegmentName) {
    final long desiredSegmentSizeBytes = streamConfig.getFlushThresholdSegmentSizeBytes();
    final long optimalSegmentSizeBytesMin = desiredSegmentSizeBytes / 2;
    final double optimalSegmentSizeBytesMax = desiredSegmentSizeBytes * 1.5;

    if (committingSegmentZKMetadata == null) { // first segment of the partition, hence committing segment is null
      if (_latestSegmentRowsToSizeRatio > 0) { // new partition group added case
        long targetSegmentNumRows = (long) (desiredSegmentSizeBytes * _latestSegmentRowsToSizeRatio);
        targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
        SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
            "Committing segment zk metadata is not available, using prev ratio {}, setting rows threshold for {} as {}",
            _latestSegmentRowsToSizeRatio, newSegmentName, targetSegmentNumRows);
        return (int) targetSegmentNumRows;
      } else {
        final int autotuneInitialRows = streamConfig.getFlushAutotuneInitialRows();
        SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
            "Committing segment zk metadata is not available, setting threshold for {} as {}", newSegmentName,
            autotuneInitialRows);
        return autotuneInitialRows;
      }
    }

    final long committingSegmentSizeBytes = committingSegmentDescriptor.getSegmentSizeBytes();
    if (committingSegmentSizeBytes <= 0 // repair segment case
        || REASON_FORCE_COMMIT_MESSAGE_RECEIVED.equals(committingSegmentDescriptor.getStopReason())) {
      String reason = committingSegmentSizeBytes <= 0 //
          ? "Committing segment size is not available" //
          : "Committing segment is due to force-commit";
      final int targetNumRows = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
      SegmentSizeBasedFlushThresholdUpdater.LOGGER.info("{}, setting thresholds from previous segment for {} as {}",
          reason, newSegmentName, targetNumRows);
      return targetNumRows;
    }

    final long timeConsumed = _clock.millis() - committingSegmentZKMetadata.getCreationTime();
    final long numRowsConsumed = committingSegmentZKMetadata.getTotalDocs();
    final int numRowsThreshold = committingSegmentZKMetadata.getSizeThresholdToFlushSegment();
    SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
        "{}: Data from committing segment: Time {}  numRows {} threshold {} segmentSize(bytes) {}",
        newSegmentName, TimeUtils.convertMillisToPeriod(timeConsumed), numRowsConsumed, numRowsThreshold,
        committingSegmentSizeBytes);

    double currentRatio = (double) numRowsConsumed / committingSegmentSizeBytes;
    if (_latestSegmentRowsToSizeRatio > 0) {
      _latestSegmentRowsToSizeRatio =
          CURRENT_SEGMENT_RATIO_WEIGHT * currentRatio + PREVIOUS_SEGMENT_RATIO_WEIGHT * _latestSegmentRowsToSizeRatio;
    } else {
      _latestSegmentRowsToSizeRatio = currentRatio;
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
    if (numRowsConsumed < numRowsThreshold && committingSegmentSizeBytes < desiredSegmentSizeBytes) {
      final long timeThresholdMillis = streamConfig.getFlushThresholdTimeMillis();
      long currentNumRows = numRowsConsumed;
      StringBuilder logStringBuilder = new StringBuilder().append("Time threshold reached. ");
      if (timeThresholdMillis < timeConsumed) {
        // The administrator has reduced the time threshold. Adjust the
        // number of rows to match the average consumption rate on the partition.
        currentNumRows = timeThresholdMillis * numRowsConsumed / timeConsumed;
        logStringBuilder.append(" Detected lower time threshold, adjusting numRowsConsumed to ").append(currentNumRows)
            .append(". ");
      }
      long targetSegmentNumRows = (long) (currentNumRows * ROWS_MULTIPLIER_WHEN_TIME_THRESHOLD_HIT);
      targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
      logStringBuilder.append("Setting segment size for {} as {}");
      SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(logStringBuilder.toString(),
          newSegmentName, targetSegmentNumRows);
      return (int) targetSegmentNumRows;
    }

    long targetSegmentNumRows;
    if (committingSegmentSizeBytes < optimalSegmentSizeBytesMin) {
      targetSegmentNumRows = numRowsConsumed + numRowsConsumed / 2;
    } else if (committingSegmentSizeBytes > optimalSegmentSizeBytesMax) {
      targetSegmentNumRows = numRowsConsumed / 2;
    } else {
      if (_latestSegmentRowsToSizeRatio > 0) {
        targetSegmentNumRows = (long) (desiredSegmentSizeBytes * _latestSegmentRowsToSizeRatio);
      } else {
        targetSegmentNumRows = (long) (desiredSegmentSizeBytes * currentRatio);
      }
    }
    targetSegmentNumRows = capNumRowsIfOverflow(targetSegmentNumRows);
    SegmentSizeBasedFlushThresholdUpdater.LOGGER.info(
        "Committing segment size {}, current ratio {}, setting threshold for {} as {}",
        committingSegmentSizeBytes, _latestSegmentRowsToSizeRatio, newSegmentName, targetSegmentNumRows);

    return (int) targetSegmentNumRows;
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
