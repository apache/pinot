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
package org.apache.pinot.core.operator.transform.transformer.datetime;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.pinot.core.operator.transform.transformer.DataTransformer;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeFormatUnitSpec.DateTimeTransformUnit;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;


/**
 * Base date time transformer to transform and bucket date time values from epoch/simple date format to epoch/simple
 * date format.
 * <p>NOTE: time size and time unit do not apply to simple date format.
 */
public abstract class BaseDateTimeTransformer<I, O> implements DataTransformer<I, O> {
  private final int _inputTimeSize;
  private final TimeUnit _inputTimeUnit;
  private final DateTimeFormatter _inputDateTimeFormatter;
  private final int _outputTimeSize;
  private final DateTimeTransformUnit _outputTimeUnit;
  private final DateTimeFormatter _outputDateTimeFormatter;
  private final DateTimeGranularitySpec _outputGranularity;
  private final long _outputGranularityMillis;
  private final SDFDateTimeTruncate _dateTimeTruncate;

  private interface SDFDateTimeTruncate {
    String truncate(DateTime dateTime);
  }

  public BaseDateTimeTransformer(@Nonnull DateTimeFormatSpec inputFormat, @Nonnull DateTimeFormatSpec outputFormat,
      @Nonnull DateTimeGranularitySpec outputGranularity) {
    _inputTimeSize = inputFormat.getColumnSize();
    _inputTimeUnit = inputFormat.getColumnUnit();
    _inputDateTimeFormatter = inputFormat.getDateTimeFormatter();
    _outputTimeSize = outputFormat.getColumnSize();
    _outputTimeUnit = outputFormat.getColumnDateTimeTransformUnit();
    _outputDateTimeFormatter = outputFormat.getDateTimeFormatter();
    _outputGranularity = outputGranularity;
    _outputGranularityMillis = outputGranularity.granularityToMillis();

    // setup date time truncating based on output granularity
    final int sz = _outputGranularity.getSize();
    switch (_outputGranularity.getTimeUnit()) {
      case MILLISECONDS:
        _dateTimeTruncate = (dateTime) -> _outputDateTimeFormatter
            .print(dateTime.withMillisOfSecond((dateTime.getMillisOfSecond() / sz) * sz));
        break;
      case SECONDS:
        _dateTimeTruncate = (dateTime) -> _outputDateTimeFormatter.print(
            dateTime.withSecondOfMinute((dateTime.getSecondOfMinute() / sz) * sz).secondOfMinute().roundFloorCopy());
        break;
      case MINUTES:
        _dateTimeTruncate = (dateTime) -> _outputDateTimeFormatter
            .print(dateTime.withMinuteOfHour((dateTime.getMinuteOfHour() / sz) * sz).minuteOfHour().roundFloorCopy());
        break;
      case HOURS:
        _dateTimeTruncate = (dateTime) -> _outputDateTimeFormatter
            .print(dateTime.withHourOfDay((dateTime.getHourOfDay() / sz) * sz).hourOfDay().roundFloorCopy());
        break;
      case DAYS:
        _dateTimeTruncate = (dateTime) -> _outputDateTimeFormatter
            .print(dateTime.withDayOfMonth((dateTime.getDayOfMonth() / sz) * sz).dayOfMonth().roundFloorCopy());
        break;
      default:
        _dateTimeTruncate = _outputDateTimeFormatter::print;
    }
  }

  protected long transformEpochToMillis(long epochTime) {
    return _inputTimeUnit.toMillis(epochTime * _inputTimeSize);
  }

  protected long transformSDFToMillis(@Nonnull String sdfTime) {
    return _inputDateTimeFormatter.parseMillis(sdfTime);
  }

  protected long transformMillisToEpoch(long millisSinceEpoch) {
    return _outputTimeUnit.fromMillis(millisSinceEpoch) / _outputTimeSize;
  }

  protected String transformMillisToSDF(long millisSinceEpoch) {
    // convert to date time (with timezone), then truncate/bucket to the desired output granularity
    return _dateTimeTruncate.truncate(new DateTime(millisSinceEpoch, _outputDateTimeFormatter.getZone()));
  }

  protected long transformToOutputGranularity(long millisSinceEpoch) {
    return (millisSinceEpoch / _outputGranularityMillis) * _outputGranularityMillis;
  }
}
