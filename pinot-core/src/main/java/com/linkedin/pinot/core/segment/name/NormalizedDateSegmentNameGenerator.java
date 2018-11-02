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
package com.linkedin.pinot.core.segment.name;

import com.google.common.base.Joiner;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.segment.creator.ColumnStatistics;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment name generator that normalizes the date to human readable format.
 */
public class NormalizedDateSegmentNameGenerator implements SegmentNameGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizedDateSegmentNameGenerator.class);

  private final String _tableName;
  private final int _sequenceId;
  private final String _timeColumnType;
  private final String _tablePushFrequency;
  private final String _tablePushType;
  private final String _segmentNamePrefix;
  private final String _segmentExcludeSequenceId;
  private final String _schemaTimeFormat;

  public NormalizedDateSegmentNameGenerator(
      String tableName, int sequenceId, String timeColumnType, String tablePushFrequency, String tablePushType,
      String segmentNamePrefix, String segmentExcludeSequenceId, String schemaTimeFormat) {
    _tableName = tableName;
    _sequenceId = sequenceId;
    _timeColumnType = timeColumnType;
    _tablePushFrequency = tablePushFrequency;
    _tablePushType = tablePushType;
    _segmentNamePrefix = segmentNamePrefix;
    _segmentExcludeSequenceId = segmentExcludeSequenceId;
    _schemaTimeFormat = schemaTimeFormat;
  }

  @Override
  public String generateSegmentName(ColumnStatistics statsCollector) throws Exception {
    long minTimeValue = Long.parseLong(statsCollector.getMinValue().toString());
    long maxTimeValue = Long.parseLong(statsCollector.getMaxValue().toString());
    String segmentName = generateSegmentName(minTimeValue, maxTimeValue);
    LOGGER.info("Segment name is: " + segmentName + " for table name: " + _tableName);
    return segmentName;
  }

  /**
   * Generate the segment name given raw min, max time value
   *
   * @param minTimeValue min time value from a segment
   * @param maxTimeValue max time value from a segment
   * @return segment name with normalized time
   */
  public String generateSegmentName(long minTimeValue, long maxTimeValue) {
    // Use table name for prefix by default unless explicitly set
    String segmentPrefix = _tableName;
    if (_segmentNamePrefix != null) {
      LOGGER.info("Using prefix for table " + _tableName + ", prefix " + _segmentNamePrefix);
      segmentPrefix = _segmentNamePrefix;
    } else {
      LOGGER.info("Using default naming scheme for " + _tableName);
    }

    String sequenceId = Integer.toString(_sequenceId);
    Boolean excludeSequenceId = Boolean.valueOf(_segmentExcludeSequenceId);
    if (excludeSequenceId) {
      sequenceId = null;
    }

    String minTimeValueNormalized = null;
    String maxTimeValueNormalized = null;
    // For append use cases, we add dates; otherwise, we don't
    if (_tablePushType.equalsIgnoreCase("APPEND")) {
      minTimeValueNormalized = getNormalizedDate(minTimeValue);
      maxTimeValueNormalized = getNormalizedDate(maxTimeValue);
      LOGGER.info("Table push type is append. Min time value = " + minTimeValueNormalized
          + " and max time value = " + maxTimeValueNormalized + " table name: " + _tableName);
    } else {
      LOGGER.info("Table push type is refresh for table: " + _tableName);
    }
    return Joiner.on("_").skipNulls()
        .join(Arrays.asList(segmentPrefix.trim(), minTimeValueNormalized, maxTimeValueNormalized, sequenceId));
  }

  /**
   * Convert the raw time value into human readable date format
   *
   * @param timeValue time column value
   * @return normalized date string
   */
  private String getNormalizedDate(long timeValue) {
    Date sinceEpoch = null;
    if (_schemaTimeFormat.equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
      TimeUnit timeUnit = TimeUnit.valueOf(_timeColumnType);

      switch (timeUnit) {
        case MILLISECONDS:
          sinceEpoch = new Date(timeValue);
          break;
        case SECONDS:
          sinceEpoch = new Date(TimeUnit.SECONDS.toMillis(timeValue));
          break;
        case MINUTES:
          sinceEpoch = new Date(TimeUnit.MINUTES.toMillis(timeValue));
          break;
        case HOURS:
          sinceEpoch = new Date(TimeUnit.HOURS.toMillis(timeValue));
          break;
        case DAYS:
          sinceEpoch = new Date(TimeUnit.DAYS.toMillis(timeValue));
          break;
      }
    } else {
      try {
        SimpleDateFormat dateFormatPassedIn = new SimpleDateFormat(_schemaTimeFormat);
        dateFormatPassedIn.setTimeZone(TimeZone.getTimeZone("UTC"));
        sinceEpoch = dateFormatPassedIn.parse(Long.toString(timeValue));
      } catch (Exception e) {
        throw new RuntimeException("Could not parse simple date format: '" + _schemaTimeFormat);
      }
    }

    // Pick the current time when having a parsing error
    if (sinceEpoch == null) {
      LOGGER.warn("Could not translate timeType '" + _timeColumnType);
      sinceEpoch = new Date();
    }

    // Get date format
    SimpleDateFormat dateFormat;
    if (_tablePushFrequency.equalsIgnoreCase("HOURLY")) {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
    } else {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    }
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    return dateFormat.format(sinceEpoch);
  }
}
