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
package org.apache.pinot.common.utils;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utility methods for evaluating segment retention eligibility and parsing table data retention from config.
public class RetentionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionUtils.class);

  private RetentionUtils() {
  }

  /// Whether time-based data retention should run for this table's segments, matching the controller retention manager:
  /// for {@link TableType#OFFLINE} tables, retention applies only when batch segment ingestion type is {@code APPEND}.
  public static boolean shouldManageTimeBasedDataRetention(TableConfig tableConfig) {
    if (tableConfig.getTableType() == TableType.OFFLINE && !"APPEND"
        .equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))) {
      return false;
    }
    return true;
  }

  /// Implements the time-comparison and creation-time fallback logic used by `TimeRetentionStrategy` for completed
  /// segments. Does **not** check segment completion status — callers must guarantee that only completed segments (DONE
  /// or UPLOADED) are passed in.
  ///
  /// - If end time is valid: expired when `currentTimeMs - endTimeMs > retentionMs`.
  /// - If end time is invalid and `useCreationTimeFallback` is true and creation time is valid: expired when
  ///   `currentTimeMs - creationTimeMs > retentionMs`.
  /// - Otherwise: not expired (fail-open).
  ///
  /// @param tableNameWithType table name with type suffix, used for logging
  /// @param segmentZKMetadata segment metadata
  /// @param retentionMs retention period in milliseconds (must be positive)
  /// @param currentTimeMs current wall-clock time in milliseconds
  /// @param useCreationTimeFallback when true, fall back to creation time if end time is invalid (must match
  ///        `controller.retentionManager.enableCreationTimeFallback`)
  /// @return `true` if the segment is past the retention boundary, `false` otherwise
  public static boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata, long retentionMs,
      long currentTimeMs, boolean useCreationTimeFallback) {
    return isPurgeableInternal(segmentZKMetadata.getEndTimeMs(), segmentZKMetadata.getCreationTime(), retentionMs,
        currentTimeMs, useCreationTimeFallback, tableNameWithType, segmentZKMetadata.getSegmentName());
  }

  /// Whether segment file metadata is past the retention window, using end time from
  /// {@link #getSegmentMetadataEndTimeMillis(SegmentMetadata)} and
  /// {@link SegmentMetadata#getIndexCreationTime()} as the optional fallback clock. Invalid-end paths emit the same
  /// style of debug/warn lines as the ZK overload, using
  /// {@link SegmentMetadata#getName()} for the segment in log messages.
  ///
  /// - If end time is valid: purgeable when `currentTimeMs - endTimeMs > retentionMs`.
  /// - If end time is invalid and `useCreationTimeFallback` is true and index creation time is valid: purgeable when
  ///   `currentTimeMs - creationTimeMs > retentionMs`.
  /// - Otherwise: not purgeable (fail-open).
  public static boolean isPurgeable(String tableNameWithType, SegmentMetadata segmentMetadata, long retentionMs,
      long currentTimeMs, boolean useCreationTimeFallback) {
    return isPurgeableInternal(getSegmentMetadataEndTimeMillis(segmentMetadata), segmentMetadata.getIndexCreationTime(),
        retentionMs, currentTimeMs, useCreationTimeFallback, tableNameWithType, segmentMetadata.getName());
  }

  private static boolean isPurgeableInternal(long endTimeMs, long creationTimeMs, long retentionMs,
      long currentTimeMs, boolean useCreationTimeFallback, String tableNameWithType,
      String segmentName) {
    if (TimeUtils.timeValueInValidRange(endTimeMs)) {
      return currentTimeMs - endTimeMs > retentionMs;
    }
    if (useCreationTimeFallback && TimeUtils.timeValueInValidRange(creationTimeMs)) {
      LOGGER.debug("Segment: {} of table: {} has invalid end time: {}. Using creation time: {} as fallback",
          segmentName, tableNameWithType, endTimeMs, creationTimeMs);
      return currentTimeMs - creationTimeMs > retentionMs;
    }
    if (useCreationTimeFallback) {
      LOGGER.warn("Segment: {} of table: {} has invalid end time: {} and invalid creation time: {}. "
          + "Cannot determine retention, skipping", segmentName, tableNameWithType, endTimeMs, creationTimeMs);
    } else {
      LOGGER.warn("Segment: {} of table: {} has invalid end time in millis: {}. "
          + "Creation time fallback is disabled", segmentName, tableNameWithType, endTimeMs);
    }
    return false;
  }

  /// Parses {@link SegmentsValidationAndRetentionConfig#getRetentionTimeUnit()} and
  /// {@link SegmentsValidationAndRetentionConfig#getRetentionTimeValue()} into table data retention duration in
  /// milliseconds (same interpretation as controller time-based retention).
  ///
  /// @return millis, or empty if unset or not parseable
  public static OptionalLong parseTableDataRetentionMillis(
      @Nullable SegmentsValidationAndRetentionConfig validationConfig) {
    if (validationConfig == null) {
      return OptionalLong.empty();
    }
    String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
    String retentionTimeValue = validationConfig.getRetentionTimeValue();
    if (StringUtils.isEmpty(retentionTimeUnit) || StringUtils.isEmpty(retentionTimeValue)) {
      return OptionalLong.empty();
    }
    try {
      return OptionalLong.of(TimeUnit.valueOf(retentionTimeUnit.toUpperCase())
          .toMillis(Long.parseLong(retentionTimeValue)));
    } catch (Exception e) {
      LOGGER.warn("Failed to parse table data retention: unit='{}' value='{}'", retentionTimeUnit, retentionTimeValue,
          e);
      return OptionalLong.empty();
    }
  }

  /// Returns segment end time in epoch millis from on-disk {@link SegmentMetadata} for retention comparison, using
  /// {@link SegmentMetadata#getTimeUnit()} and {@link SegmentMetadata#getEndTime()} (same raw fields minion task
  /// executors pass when configuring generation from segment files). When `timeUnit` is non-null and raw end time is
  /// not {@link Long#MIN_VALUE}, returns `timeUnit.toMillis(endTime)`; otherwise returns the raw end time (typically
  /// `Long.MIN_VALUE` when unset).
  public static long getSegmentMetadataEndTimeMillis(SegmentMetadata segmentMetadata) {
    TimeUnit timeUnit = segmentMetadata.getTimeUnit();
    long endTime = segmentMetadata.getEndTime();
    if (timeUnit != null && endTime != Long.MIN_VALUE) {
      return timeUnit.toMillis(endTime);
    }
    return endTime;
  }
}
