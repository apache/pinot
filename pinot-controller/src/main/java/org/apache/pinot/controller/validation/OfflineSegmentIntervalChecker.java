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
package org.apache.pinot.controller.validation;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.util.SegmentIntervalUtils;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.base.BaseInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment validation metrics, to ensure that all offline segments are contiguous (no missing segments) and
 * that the offline push delay isn't too high.
 */
public class OfflineSegmentIntervalChecker extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OfflineSegmentIntervalChecker.class);

  private final ValidationMetrics _validationMetrics;
  private final boolean _segmentAutoResetOnErrorAtValidation;

  public OfflineSegmentIntervalChecker(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ValidationMetrics validationMetrics,
      ControllerMetrics controllerMetrics) {
    super("OfflineSegmentIntervalChecker", config.getOfflineSegmentIntervalCheckerFrequencyInSeconds(),
        config.getOfflineSegmentIntervalCheckerInitialDelayInSeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _validationMetrics = validationMetrics;
    _segmentAutoResetOnErrorAtValidation = config.isAutoResetErrorSegmentsOnValidationEnabled();
  }

  @Override
  protected void processTable(String tableNameWithType) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    if (tableType == TableType.OFFLINE) {

      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        LOGGER.warn("Failed to find table config for table: {}, skipping validation", tableNameWithType);
        return;
      }
      validateOfflineSegmentPush(tableConfig);
    }
  }

  // For offline segment pushes, validate that there are no missing segments, and update metrics
  private void validateOfflineSegmentPush(TableConfig tableConfig) {
    String offlineTableName = tableConfig.getTableName();
    List<SegmentZKMetadata> segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(offlineTableName);

    // Compute the missing segments if there are at least two segments and the table has time column
    int numMissingSegments = 0;
    int numSegments = segmentsZKMetadata.size();
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (SegmentIntervalUtils.eligibleForMissingSegmentCheck(numSegments, validationConfig)) {
      List<Interval> segmentIntervals = new ArrayList<>(numSegments);
      int numSegmentsWithInvalidIntervals = 0;
      for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
        long startTimeMs = segmentZKMetadata.getStartTimeMs();
        long endTimeMs = segmentZKMetadata.getEndTimeMs();
        if (TimeUtils.timeValueInValidRange(startTimeMs) && TimeUtils.timeValueInValidRange(endTimeMs)) {
          segmentIntervals.add(new Interval(startTimeMs, endTimeMs));
        } else {
          numSegmentsWithInvalidIntervals++;
        }
      }
      if (numSegmentsWithInvalidIntervals > 0) {
        LOGGER
            .warn("Table: {} has {} segments with invalid interval", offlineTableName, numSegmentsWithInvalidIntervals);
      }
      Duration frequency =
          SegmentIntervalUtils.convertToDuration(IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig));
      numMissingSegments = computeNumMissingSegments(segmentIntervals, frequency);
    }
    // Update the gauge that contains the number of missing segments
    _validationMetrics.updateMissingSegmentCountGauge(offlineTableName, numMissingSegments);

    // Compute the max segment end time and max segment push time
    long maxSegmentEndTime = Long.MIN_VALUE;
    long maxSegmentPushTime = Long.MIN_VALUE;

    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      long endTimeMs = segmentZKMetadata.getEndTimeMs();
      if (TimeUtils.timeValueInValidRange(endTimeMs) && maxSegmentEndTime < endTimeMs) {
        maxSegmentEndTime = endTimeMs;
      }

      long segmentPushTime = segmentZKMetadata.getPushTime();
      long segmentRefreshTime = segmentZKMetadata.getRefreshTime();
      long segmentUpdateTime = Math.max(segmentPushTime, segmentRefreshTime);

      if (maxSegmentPushTime < segmentUpdateTime) {
        maxSegmentPushTime = segmentUpdateTime;
      }
    }

    // Update the gauges that contain the delay between the current time and last segment end time
    _validationMetrics.updateOfflineSegmentDelayGauge(offlineTableName, maxSegmentEndTime);
    _validationMetrics.updateLastPushTimeGauge(offlineTableName, maxSegmentPushTime);
    // Update the gauge to contain the total document count in the segments
    _validationMetrics
        .updateTotalDocumentCountGauge(offlineTableName, computeOfflineTotalDocumentInSegments(segmentsZKMetadata));
    // Update the gauge to contain the total number of segments for this table
    _validationMetrics.updateSegmentCountGauge(offlineTableName, numSegments);

    if (_segmentAutoResetOnErrorAtValidation) {
      _pinotHelixResourceManager.resetSegments(offlineTableName, null, true);
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType == TableType.OFFLINE) {
        // TODO: we can further split the existing ValidationMetricName enum to OFFLINE and REALTIME,
        //  so that we can simply loop through all the enum values and clean up the metrics.
        _validationMetrics.cleanupMissingSegmentCountGauge(tableNameWithType);
        _validationMetrics.cleanupOfflineSegmentDelayGauge(tableNameWithType);
        _validationMetrics.cleanupLastPushTimeGauge(tableNameWithType);
        _validationMetrics.cleanupTotalDocumentCountGauge(tableNameWithType);
        _validationMetrics.cleanupSegmentCountGauge(tableNameWithType);
      }
    }
  }

  /**
   * Computes the number of missing segments based on the given existing segment intervals and the expected frequency
   * of the intervals.
   * <p>We count the interval as missing if there are at least two intervals between the start of the previous interval
   * and current interval. For long intervals (span over multiple intervals), count its start time as the start time of
   * the last interval it covers.
   *
   * @param segmentIntervals List of existing segment intervals
   * @param frequency Expected interval frequency
   * @return Number of missing segments
   */
  @VisibleForTesting
  static int computeNumMissingSegments(List<Interval> segmentIntervals, Duration frequency) {
    int numSegments = segmentIntervals.size();

    // If there are less than two segments, none can be missing
    if (numSegments < 2) {
      return 0;
    }

    // Sort the intervals by ascending starting time
    segmentIntervals.sort(Comparator.comparingLong(BaseInterval::getStartMillis));

    int numMissingSegments = 0;
    long frequencyMs = frequency.getMillis();
    long lastStartTimeMs = -1L;
    for (Interval segmentInterval : segmentIntervals) {
      long startTimeMs = segmentInterval.getStartMillis();
      if (lastStartTimeMs != -1L && startTimeMs - lastStartTimeMs > frequencyMs) {
        // If there are at least two intervals between the start of the previous interval and current interval, then
        // count the interval(s) as missing
        numMissingSegments += (startTimeMs - lastStartTimeMs - frequencyMs) / frequencyMs;
      }
      // Handle long intervals
      long endTimeMs = segmentInterval.getEndMillis();
      while (startTimeMs + frequencyMs <= endTimeMs) {
        startTimeMs += frequencyMs;
      }
      lastStartTimeMs = Math.max(lastStartTimeMs, startTimeMs);
    }
    return numMissingSegments;
  }

  @VisibleForTesting
  static long computeOfflineTotalDocumentInSegments(List<SegmentZKMetadata> segmentsZKMetadata) {
    long numTotalDocs = 0;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      numTotalDocs += segmentZKMetadata.getTotalDocs();
    }
    return numTotalDocs;
  }

  @VisibleForTesting
  public ValidationMetrics getValidationMetrics() {
    return _validationMetrics;
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Unregister all the validation metrics.");
    _validationMetrics.unregisterAllMetrics();
  }
}
