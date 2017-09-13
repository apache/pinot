/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.transform.function.time.converter;

import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Weeks;
import org.joda.time.Years;


/**
 * Implementation of {@link TimeUnitConverter} interface for custom time units such as
 * WEEKS, MONTHS & YEARS.
 */
public class CustomTimeUnitConverter implements TimeUnitConverter {
  private static final DateTime EPOCH_START_DATE = new DateTime(0L, DateTimeZone.UTC);

  private CustomTimeUnit _timeUnit;

  public enum CustomTimeUnit {
    WEEKS,
    MONTHS,
    YEARS
  }

  /**
   * Constructor for the class
   * @param timeUnitName String name of time unit.
   */
  public CustomTimeUnitConverter(String timeUnitName) {
    String name = timeUnitName.toUpperCase();
    switch (name) {
      case "WEEKS":
      case "MONTHS":
      case "YEARS":
        _timeUnit = CustomTimeUnit.valueOf(name);
        break;

      default:
        throw new IllegalArgumentException("Illegal time unit for time conversion: " + timeUnitName);
    }
  }

  @Override
  public void convert(long[] inputTime, TimeUnit inputTimeUnit, int length, long[] outputTime) {
    MutableDateTime inputDateTime = new MutableDateTime(0L, DateTimeZone.UTC);

    // For loop within switch better than switch within for loop (may be??).
    switch (_timeUnit) {
      case WEEKS:
        for (int i = 0; i < length; i++) {
          long inputTimeMillis = TimeUnit.MILLISECONDS.convert(inputTime[i], inputTimeUnit);
          inputDateTime.setDate(inputTimeMillis);
          outputTime[i] = Weeks.weeksBetween(EPOCH_START_DATE, inputDateTime).getWeeks();
        }
        break;

      case MONTHS:
        for (int i = 0; i < length; i++) {
          long inputTimeMillis = TimeUnit.MILLISECONDS.convert(inputTime[i], inputTimeUnit);
          inputDateTime.setDate(inputTimeMillis);
          outputTime[i] = Months.monthsBetween(EPOCH_START_DATE, inputDateTime).getMonths();
        }
        break;

      case YEARS:
        for (int i = 0; i < length; i++) {
          long inputTimeMillis = TimeUnit.MILLISECONDS.convert(inputTime[i], inputTimeUnit);
          inputDateTime.setDate(inputTimeMillis);
          outputTime[i] = Years.yearsBetween(EPOCH_START_DATE, inputDateTime).getYears();
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal argument for time unit: " + inputTimeUnit);
    }
  }
}
