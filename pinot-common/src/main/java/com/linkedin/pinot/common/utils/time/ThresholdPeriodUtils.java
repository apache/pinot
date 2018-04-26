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
package com.linkedin.pinot.common.utils.time;

import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;


/**
 * Utils class with helper methods to handle conversion between period string and millis
 */
public class ThresholdPeriodUtils {

  private static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder()
      .appendDays().appendSuffix("d")
      .appendHours().appendSuffix("h")
      .appendMinutes().appendSuffix("m")
      .appendSeconds().appendSuffix("s")
      .toFormatter();

  public static Long convertToMillis(String timeStr) {
    Long millis = 0L;
    if (timeStr != null) {
      try {
        Period p = PERIOD_FORMATTER.parsePeriod(timeStr);
        millis = p.toStandardDuration().getStandardSeconds() * 1000L;
      } catch (Exception e) {
        throw new RuntimeException("Invalid time spec '" + timeStr + "' (Valid examples: '3h', '4h30m')", e);
      }
    }
    return millis;
  }

  public static  String convertToPeriod(Long millis) {
    String periodStr = null;
    if (millis != null) {
      Period p = new Period(new Duration(millis));
      periodStr = PERIOD_FORMATTER.print(p);
    }
    return periodStr;
  }
}
