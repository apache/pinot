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

  private String _segmentNamePrefix;
  private boolean _excludeSequenceId;
  private boolean _appendPushType;

  // For APPEND tables
  private SimpleDateFormat _outputSDF;
  // For EPOCH time format
  private TimeUnit _inputTimeUnit;
  // For SIMPLE_DATE_FORMAT time format
  private SimpleDateFormat _inputSDF;

  public NormalizedDateSegmentNameGenerator(String tableName, @Nullable String segmentNamePrefix,
      boolean excludeSequenceId, @Nullable String pushType, @Nullable String pushFrequency,
      @Nullable DateTimeFormatSpec dateTimeFormatSpec) {
    _segmentNamePrefix = segmentNamePrefix != null ? segmentNamePrefix.trim() : tableName;
    Preconditions.checkArgument(
        _segmentNamePrefix != null && isValidSegmentName(_segmentNamePrefix));
    _excludeSequenceId = excludeSequenceId;
    _appendPushType = "APPEND".equalsIgnoreCase(pushType);

    // Include time info for APPEND push type
    if (_appendPushType) {
      // For HOURLY push frequency, include hours into output format
      if (PUSH_FREQUENCY_HOURLY.equalsIgnoreCase(pushFrequency)) {
        _outputSDF = new SimpleDateFormat("yyyy-MM-dd-HH");
      } else {
        _outputSDF = new SimpleDateFormat("yyyy-MM-dd");
      }
      _outputSDF.setTimeZone(TimeZone.getTimeZone("UTC"));

      // Parse input time format: 'EPOCH' or 'SIMPLE_DATE_FORMAT' using pattern
      Preconditions.checkNotNull(dateTimeFormatSpec);
      TimeFormat timeFormat = dateTimeFormatSpec.getTimeFormat();
      if (timeFormat == TimeFormat.EPOCH) {
        _inputTimeUnit = dateTimeFormatSpec.getColumnUnit();
        _inputSDF = null;
      } else {
        Preconditions.checkNotNull(dateTimeFormatSpec.getSDFPattern(), "Must provide pattern for SIMPLE_DATE_FORMAT");
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
      return JOINER.join(_segmentNamePrefix, getNormalizedDate(Preconditions.checkNotNull(minTimeValue)),
          getNormalizedDate(Preconditions.checkNotNull(maxTimeValue)), sequenceIdInSegmentName);
    } else {
      return JOINER.join(_segmentNamePrefix, sequenceIdInSegmentName);
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
        new StringBuilder("NormalizedDateSegmentNameGenerator: segmentNamePrefix=").append(_segmentNamePrefix)
            .append(", appendPushType=").append(_appendPushType);
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
