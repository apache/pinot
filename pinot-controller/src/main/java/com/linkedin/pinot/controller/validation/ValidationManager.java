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
package com.linkedin.pinot.controller.validation;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ValidationMetrics;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.core.periodictask.BasePeriodicTask;
import com.linkedin.pinot.core.periodictask.PeriodicTask;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.InstanceConfig;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the segment validation metrics, to ensure that all offline segments are contiguous (no missing segments) and
 * that the offline push delay isn't too high.
 */
public class ValidationManager extends PeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationManager.class);

  private final long _validationIntervalSeconds;
  private final boolean _enableSegmentLevelValidation;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final ValidationMetrics _validationMetrics;
  private final ScheduledExecutorService _executorService;

  public ValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager, ValidationMetrics validationMetrics) {
    super("ValidationManager", config.getValidationControllerFrequencyInSeconds());
    _validationIntervalSeconds = config.getValidationControllerFrequencyInSeconds();
    _enableSegmentLevelValidation = config.getEnableSegmentLevelValidation();
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
    _executorService =
        Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "ValidationManagerThread"));
  }

  @Override
  public void runTask() {
    runValidation();
  }

  /**
   * Runs a validation pass over the currently loaded tables.
   */
  public void runValidation() {
    if (!_pinotHelixResourceManager.isLeader()) {
      _validationMetrics.unregisterAllMetrics();
      LOGGER.info("Skipping validation, not leader!");
      return;
    }

    LOGGER.info("Starting validation");
    // Cache instance configs to reduce ZK access
    List<InstanceConfig> instanceConfigs = _pinotHelixResourceManager.getAllHelixInstanceConfigs();

    for (String tableNameWithType : _pinotHelixResourceManager.getAllTables()) {
      try {
        TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
        if (tableConfig == null) {
          LOGGER.warn("Failed to find table config for table: {}, skipping validation", tableNameWithType);
          continue;
        }

        // Rebuild broker resource
        Set<String> brokerInstances = _pinotHelixResourceManager.getAllInstancesForBrokerTenant(instanceConfigs,
            tableConfig.getTenantConfig().getBroker());
        _pinotHelixResourceManager.rebuildBrokerResource(tableNameWithType, brokerInstances);

        // Perform validation based on the table type
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
        if (tableType == TableType.OFFLINE) {
          if (_enableSegmentLevelValidation) {
            validateOfflineSegmentPush(tableConfig);
          }
        } else {
          if (_enableSegmentLevelValidation) {
            updateRealtimeDocumentCount(tableConfig);
          }
          _llcRealtimeSegmentManager.validateLLCSegments(tableConfig);
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while validating table: {}", tableNameWithType, e);
      }
    }
    LOGGER.info("Validation completed");
  }

  // For offline segment pushes, validate that there are no missing segments, and update metrics
  private void validateOfflineSegmentPush(TableConfig tableConfig) {
    String offlineTableName = tableConfig.getTableName();
    List<OfflineSegmentZKMetadata> offlineSegmentZKMetadataList =
        _pinotHelixResourceManager.getOfflineSegmentMetadata(offlineTableName);

    // Compute the missing segments if there are at least two segments and the table has time column
    int numMissingSegments = 0;
    int numSegments = offlineSegmentZKMetadataList.size();
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (numSegments >= 2 && StringUtils.isNotEmpty(validationConfig.getTimeColumnName())) {
      List<Interval> segmentIntervals = new ArrayList<>(numSegments);
      List<String> segmentsWithInvalidInterval = new ArrayList<>();
      for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadataList) {
        Interval timeInterval = offlineSegmentZKMetadata.getTimeInterval();
        if (timeInterval != null && TimeUtils.timeValueInValidRange(timeInterval.getStartMillis())
            && TimeUtils.timeValueInValidRange(timeInterval.getEndMillis())) {
          segmentIntervals.add(timeInterval);
        } else {
          segmentsWithInvalidInterval.add(offlineSegmentZKMetadata.getSegmentName());
        }
      }
      if (!segmentsWithInvalidInterval.isEmpty()) {
        LOGGER.warn("Table: {} has segments with invalid interval: {}", offlineTableName, segmentsWithInvalidInterval);
      }
      Duration frequency = convertToDuration(validationConfig.getSegmentPushFrequency());
      numMissingSegments = computeNumMissingSegments(segmentIntervals, frequency);
    }
    // Update the gauge that contains the number of missing segments
    _validationMetrics.updateMissingSegmentCountGauge(offlineTableName, numMissingSegments);

    // Compute the max segment end time and max segment push time
    long maxSegmentEndTime = Long.MIN_VALUE;
    long maxSegmentPushTime = Long.MIN_VALUE;

    for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadataList) {
      Interval segmentInterval = offlineSegmentZKMetadata.getTimeInterval();

      if (segmentInterval != null && maxSegmentEndTime < segmentInterval.getEndMillis()) {
        maxSegmentEndTime = segmentInterval.getEndMillis();
      }

      long segmentPushTime = offlineSegmentZKMetadata.getPushTime();
      long segmentRefreshTime = offlineSegmentZKMetadata.getRefreshTime();
      long segmentUpdateTime = Math.max(segmentPushTime, segmentRefreshTime);

      if (maxSegmentPushTime < segmentUpdateTime) {
        maxSegmentPushTime = segmentUpdateTime;
      }
    }

    // Update the gauges that contain the delay between the current time and last segment end time
    _validationMetrics.updateOfflineSegmentDelayGauge(offlineTableName, maxSegmentEndTime);
    _validationMetrics.updateLastPushTimeGauge(offlineTableName, maxSegmentPushTime);
    // Update the gauge to contain the total document count in the segments
    _validationMetrics.updateTotalDocumentCountGauge(offlineTableName,
        computeOfflineTotalDocumentInSegments(offlineSegmentZKMetadataList));
    // Update the gauge to contain the total number of segments for this table
    _validationMetrics.updateSegmentCountGauge(offlineTableName, numSegments);
  }

  /**
   * Converts push frequency into duration. For invalid or less than 'hourly' push frequency, treats it as 'daily'.
   */
  private Duration convertToDuration(String pushFrequency) {
    if ("hourly".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardHours(1L);
    }
    if ("daily".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardDays(1L);
    }
    if ("weekly".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardDays(7L);
    }
    if ("monthly".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardDays(30L);
    }
    return Duration.standardDays(1L);
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
    segmentIntervals.sort((o1, o2) -> Long.compare(o1.getStartMillis(), o2.getStartMillis()));

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
  static long computeOfflineTotalDocumentInSegments(List<OfflineSegmentZKMetadata> offlineSegmentZKMetadataList) {
    long numTotalDocs = 0;
    for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadataList) {
      numTotalDocs += offlineSegmentZKMetadata.getTotalRawDocs();
    }
    return numTotalDocs;
  }

  private void updateRealtimeDocumentCount(TableConfig tableConfig) {
    String realtimeTableName = tableConfig.getTableName();
    List<RealtimeSegmentZKMetadata> metadataList =
        _pinotHelixResourceManager.getRealtimeSegmentMetadata(realtimeTableName);
    boolean countHLCSegments = true;  // false if this table has ONLY LLC segments (i.e. fully migrated)
    StreamConfig streamConfig = new StreamConfig(tableConfig.getIndexingConfig().getStreamConfigs());
    if (streamConfig.hasLowLevelConsumerType() && !streamConfig.hasHighLevelConsumerType()) {
      countHLCSegments = false;
    }
    // Update the gauge to contain the total document count in the segments
    _validationMetrics.updateTotalDocumentCountGauge(tableConfig.getTableName(),
        computeRealtimeTotalDocumentInSegments(metadataList, countHLCSegments));
  }

  @VisibleForTesting
  static long computeRealtimeTotalDocumentInSegments(List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadataList,
      boolean countHLCSegments) {
    long numTotalDocs = 0;

    String groupId = "";
    for (RealtimeSegmentZKMetadata realtimeSegmentZKMetadata : realtimeSegmentZKMetadataList) {
      String segmentName = realtimeSegmentZKMetadata.getSegmentName();
      if (SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
        if (countHLCSegments) {
          HLCSegmentName hlcSegmentName = new HLCSegmentName(segmentName);
          String segmentGroupIdName = hlcSegmentName.getGroupId();

          if (groupId.isEmpty()) {
            groupId = segmentGroupIdName;
          }
          // Discard all segments with different groupids as they are replicas
          if (groupId.equals(segmentGroupIdName) && realtimeSegmentZKMetadata.getTotalRawDocs() >= 0) {
            numTotalDocs += realtimeSegmentZKMetadata.getTotalRawDocs();
          }
        }
      } else {
        // Low level segments
        if (!countHLCSegments) {
          numTotalDocs += realtimeSegmentZKMetadata.getTotalRawDocs();
        }
      }
    }

    return numTotalDocs;
  }

  /**
   * Computes a list of missing intervals, given a list of existing intervals and the expected frequency of the
   * intervals.
   *
   * @param segmentIntervals The list of existing intervals
   * @param frequency The expected interval frequency
   * @return The list of missing intervals
   */
  public static List<Interval> computeMissingIntervals(List<Interval> segmentIntervals, Duration frequency) {
    // Sanity check for frequency
    if (frequency == null) {
      return Collections.emptyList();
    }

    // Default segment granularity to day level if its small than hours.
    if (frequency.getMillis() < Duration.standardHours(1).getMillis()) {
      frequency = Duration.standardDays(1);
    }

    // If there are less than two segments, none can be missing
    if (segmentIntervals.size() < 2) {
      return Collections.emptyList();
    }

    // Sort the intervals by ascending starting time
    List<Interval> sortedSegmentIntervals = new ArrayList<Interval>(segmentIntervals);
    Collections.sort(sortedSegmentIntervals, new Comparator<Interval>() {
      @Override
      public int compare(Interval first, Interval second) {
        if (first.getStartMillis() < second.getStartMillis()) {
          return -1;
        } else if (second.getStartMillis() < first.getStartMillis()) {
          return 1;
        }
        return 0;
      }
    });

    // Find the minimum starting time and maximum ending time
    final long startTime = sortedSegmentIntervals.get(0).getStartMillis();
    long endTime = Long.MIN_VALUE;
    for (Interval sortedSegmentInterval : sortedSegmentIntervals) {
      if (endTime < sortedSegmentInterval.getEndMillis()) {
        endTime = sortedSegmentInterval.getEndMillis();
      }
    }

    final long frequencyMillis = frequency.getMillis();
    int lastEndIntervalCount = 0;
    List<Interval> missingIntervals = new ArrayList<Interval>(10);
    for (Interval segmentInterval : sortedSegmentIntervals) {
      int startIntervalCount = (int) ((segmentInterval.getStartMillis() - startTime) / frequencyMillis);
      int endIntervalCount = (int) ((segmentInterval.getEndMillis() - startTime) / frequencyMillis);

      // If there is at least one complete missing interval between the end of the previous interval and the start of
      // the current interval, then mark the missing interval(s) as missing
      if (lastEndIntervalCount < startIntervalCount - 1) {
        for (int missingIntervalIndex = lastEndIntervalCount + 1; missingIntervalIndex < startIntervalCount;
            ++missingIntervalIndex) {
          missingIntervals.add(new Interval(startTime + frequencyMillis * missingIntervalIndex,
              startTime + frequencyMillis * (missingIntervalIndex + 1) - 1));
        }
      }

      lastEndIntervalCount = Math.max(lastEndIntervalCount, endIntervalCount);
    }

    return missingIntervals;
  }

  /**
   * Counts the number of missing segments, given their start times and their expected frequency.
   *
   * @param sortedStartTimes Start times for the segments, sorted in ascending order.
   * @param frequency The expected segment frequency (ie. daily, hourly, etc.)
   */
  public static int countMissingSegments(long[] sortedStartTimes, TimeUnit frequency) {
    // If there are less than two segments, none can be missing
    if (sortedStartTimes.length < 2) {
      return 0;
    }

    final long frequencyMillis = frequency.toMillis(1);
    final long halfFrequencyMillis = frequencyMillis / 2;
    final long firstStartTime = sortedStartTimes[0];
    final long lastStartTime = sortedStartTimes[sortedStartTimes.length - 1];
    final int expectedSegmentCount = (int) ((lastStartTime + halfFrequencyMillis - firstStartTime) / frequencyMillis);

    int missingSegments = 0;
    int currentIndex = 1;
    int expectedIntervalCount = 1;
    while (expectedIntervalCount <= expectedSegmentCount) {
      // Count the number of complete intervals that are found
      final int intervalCount =
          (int) ((sortedStartTimes[currentIndex] + halfFrequencyMillis - firstStartTime) / frequencyMillis);

      // Does this segment have the expected interval count?
      if (intervalCount == expectedIntervalCount) {
        // Yes, advance both the current index and expected interval count
        ++expectedIntervalCount;
        ++currentIndex;
      } else {
        if (intervalCount < expectedIntervalCount) {
          // Duplicate segment, just advance the index
          ++currentIndex;
        } else {
          // Missing segment(s), advance the index, increment the number of missing segments by the number of missing
          // intervals and set the expected interval to the following one
          missingSegments += intervalCount - expectedIntervalCount;
          expectedIntervalCount = intervalCount + 1;
          ++currentIndex;
        }
      }
    }

    return missingSegments;
  }
}
