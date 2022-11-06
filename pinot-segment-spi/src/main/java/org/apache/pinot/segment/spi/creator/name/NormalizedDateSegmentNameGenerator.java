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
package org.apache.pinot.segment.spi.creator.name;

import com.google.common.base.Preconditions;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;


/**
 * Segment name generator that normalizes the date to human readable format.
 */
@SuppressWarnings("serial")
public class NormalizedDateSegmentNameGenerator implements SegmentNameGenerator {
  // TODO: This we defined in CommonConstants in common module. SPI should depend on common, so copying here for now,
  // we will need to create a new top level module for such constants and define them there.
  private static final String PUSH_FREQUENCY_HOURLY = "hourly";

  private final String _segmentNamePrefix;
  private final boolean _excludeSequenceId;
  private final boolean _appendPushType;
  private final String _segmentNamePostfix;
  private final boolean _appendUUIDToSegmentName;

  // For APPEND tables
  private final SimpleDateFormat _outputSDF;
  // For EPOCH time format
  private final TimeUnit _inputTimeUnit;
  // For SIMPLE_DATE_FORMAT time format
  private final SimpleDateFormat _inputSDF;

  public NormalizedDateSegmentNameGenerator(String tableName, @Nullable String segmentNamePrefix,
      boolean excludeSequenceId, @Nullable String pushType, @Nullable String pushFrequency,
      @Nullable DateTimeFormatSpec dateTimeFormatSpec, @Nullable String segmentNamePostfix) {
    this(tableName, segmentNamePrefix, excludeSequenceId, pushType, pushFrequency, dateTimeFormatSpec,
        segmentNamePostfix, false);
  }

  public NormalizedDateSegmentNameGenerator(String tableName, @Nullable String segmentNamePrefix,
      boolean excludeSequenceId, @Nullable String pushType, @Nullable String pushFrequency,
      @Nullable DateTimeFormatSpec dateTimeFormatSpec, @Nullable String segmentNamePostfix,
      boolean appendUUIDToSegmentName) {
    _segmentNamePrefix = segmentNamePrefix != null ? segmentNamePrefix.trim() : tableName;
    Preconditions
        .checkArgument(_segmentNamePrefix != null, "Missing segmentNamePrefix for NormalizedDateSegmentNameGenerator");
    SegmentNameUtils.validatePartialOrFullSegmentName(_segmentNamePrefix);
    _excludeSequenceId = excludeSequenceId;
    _appendPushType = "APPEND".equalsIgnoreCase(pushType);
    _segmentNamePostfix = segmentNamePostfix != null ? segmentNamePostfix.trim() : null;
    _appendUUIDToSegmentName = appendUUIDToSegmentName;
    if (_segmentNamePostfix != null) {
      SegmentNameUtils.validatePartialOrFullSegmentName(_segmentNamePostfix);
    }

    // Include time info for APPEND push type
    if (_appendPushType) {
      // For HOURLY push frequency, include hours into output format
      if (PUSH_FREQUENCY_HOURLY.equalsIgnoreCase(pushFrequency)) {
        _outputSDF = new SimpleDateFormat("yyyy-MM-dd-HH");
      } else {
        _outputSDF = new SimpleDateFormat("yyyy-MM-dd");
      }
      _outputSDF.setTimeZone(TimeZone.getTimeZone("UTC"));

      // Parse input time format: 'EPOCH'/'TIMESTAMP' or 'SIMPLE_DATE_FORMAT' using pattern
      Preconditions.checkArgument(dateTimeFormatSpec != null,
          "Must provide date time format spec for NormalizedDateSegmentNameGenerator");
      TimeFormat timeFormat = dateTimeFormatSpec.getTimeFormat();
      if (timeFormat == TimeFormat.EPOCH || timeFormat == TimeFormat.TIMESTAMP) {
        _inputTimeUnit = dateTimeFormatSpec.getColumnUnit();
        _inputSDF = null;
      } else {
        Preconditions.checkArgument(dateTimeFormatSpec.getSDFPattern() != null,
            "Must provide pattern for SIMPLE_DATE_FORMAT for NormalizedDateSegmentNameGenerator");
        _inputTimeUnit = null;
        _inputSDF = new SimpleDateFormat(dateTimeFormatSpec.getSDFPattern());
        _inputSDF.setTimeZone(TimeZone.getTimeZone("UTC"));
      }
    } else {
      _outputSDF = null;
      _inputTimeUnit = null;
      _inputSDF = null;
    }
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    Integer sequenceIdInSegmentName = !_excludeSequenceId && sequenceId >= 0 ? sequenceId : null;

    // Include time value for APPEND push type
    if (_appendPushType) {
      Preconditions.checkArgument(minTimeValue != null, "Missing minTimeValue for NormalizedDateSegmentNameGenerator");
      Preconditions.checkArgument(maxTimeValue != null, "Missing maxTimeValue for NormalizedDateSegmentNameGenerator");
      return JOINER.join(_segmentNamePrefix, getNormalizedDate(minTimeValue), getNormalizedDate(maxTimeValue),
          _segmentNamePostfix, sequenceIdInSegmentName, _appendUUIDToSegmentName ? UUID.randomUUID() : null);
    } else {
      return JOINER.join(_segmentNamePrefix, _segmentNamePostfix, sequenceIdInSegmentName,
          _appendUUIDToSegmentName ? UUID.randomUUID() : null);
    }
  }

  /**
   * Converts the time value into human readable date format.
   *
   * @param timeValue Time value
   * @return Normalized date string
   */
  public String getNormalizedDate(Object timeValue) {
    if (_inputTimeUnit != null) {
      return _outputSDF.format(new Date(_inputTimeUnit.toMillis(Long.parseLong(timeValue.toString()))));
    } else {
      try {
        return _outputSDF.format(_inputSDF.parse(timeValue.toString()));
      } catch (ParseException e) {
        throw new RuntimeException(String
            .format("Caught exception while parsing simple date format: %s with value: %s", _inputSDF.toPattern(),
                timeValue), e);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder =
        new StringBuilder("NormalizedDateSegmentNameGenerator: segmentNamePrefix=").append(_segmentNamePrefix);
    if (_segmentNamePostfix != null) {
      stringBuilder.append(", segmentNamePostfix=").append(_segmentNamePostfix);
    }
    stringBuilder.append(", appendPushType=").append(_appendPushType);
    if (_excludeSequenceId) {
      stringBuilder.append(", excludeSequenceId=true");
    }
    if (_outputSDF != null) {
      stringBuilder.append(", outputSDF=").append(_outputSDF.toPattern());
    }
    if (_inputTimeUnit != null) {
      stringBuilder.append(", inputTimeUnit=").append(_inputTimeUnit);
    }
    if (_inputSDF != null) {
      stringBuilder.append(", inputSDF=").append(_inputSDF.toPattern());
    }
    return stringBuilder.toString();
  }
}
