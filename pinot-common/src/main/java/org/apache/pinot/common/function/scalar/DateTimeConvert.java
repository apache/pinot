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
package org.apache.pinot.common.function.scalar;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;


/**
 * Equivalent to {@code DateTimeConversionTransformFunction}.
 */
public class DateTimeConvert {
  private DateTimeFormatSpec _inputFormatSpec;
  private DateTimeFormatSpec _outputFormatSpec;
  private DateTimeGranularitySpec _granularitySpec;

  @ScalarFunction(names = {"dateTimeConvert", "date_time_convert"})
  public Object dateTimeConvert(String timeValueStr, String inputFormatStr, String outputFormatStr,
      String outputGranularityStr) {
    if (_inputFormatSpec == null) {
      _inputFormatSpec = new DateTimeFormatSpec(inputFormatStr);
      _outputFormatSpec = new DateTimeFormatSpec(outputFormatStr);
      _granularitySpec = new DateTimeGranularitySpec(outputGranularityStr);
    }

    long timeValueMs = _inputFormatSpec.fromFormatToMillis(timeValueStr);
    if (_outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT) {
      DateTimeFormatter outputFormatter = _outputFormatSpec.getDateTimeFormatter();
      DateTime dateTime = new DateTime(timeValueMs, outputFormatter.getZone());
      int size = _granularitySpec.getSize();
      switch (_granularitySpec.getTimeUnit()) {
        case MILLISECONDS:
          dateTime = dateTime.withMillisOfSecond((dateTime.getMillisOfSecond() / size) * size);
          break;
        case SECONDS:
          dateTime = dateTime.withSecondOfMinute((dateTime.getSecondOfMinute() / size) * size).secondOfMinute()
              .roundFloorCopy();
          break;
        case MINUTES:
          dateTime =
              dateTime.withMinuteOfHour((dateTime.getMinuteOfHour() / size) * size).minuteOfHour().roundFloorCopy();
          break;
        case HOURS:
          dateTime = dateTime.withHourOfDay((dateTime.getHourOfDay() / size) * size).hourOfDay().roundFloorCopy();
          break;
        case DAYS:
          dateTime =
              dateTime.withDayOfMonth(((dateTime.getDayOfMonth() - 1) / size) * size + 1).dayOfMonth().roundFloorCopy();
          break;
        default:
          break;
      }
      return outputFormatter.print(dateTime);
    } else {
      long granularityMs = _granularitySpec.granularityToMillis();
      long roundedTimeValueMs = timeValueMs / granularityMs * granularityMs;
      if (_outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.EPOCH) {
        return _outputFormatSpec.getColumnUnit().convert(roundedTimeValueMs, TimeUnit.MILLISECONDS)
            / _outputFormatSpec.getColumnSize();
      }
      // _outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.TIMESTAMP
      return _outputFormatSpec.fromMillisToFormat(roundedTimeValueMs);
    }
  }
}
