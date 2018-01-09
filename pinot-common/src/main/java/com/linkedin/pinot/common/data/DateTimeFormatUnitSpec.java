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
package com.linkedin.pinot.common.data;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.EnumUtils;


public class DateTimeFormatUnitSpec {

  /**
   * Time unit enum with range from MILLISECONDS to YEARS
   */
  public enum DateTimeTransformUnit {
    YEARS,
    MONTHS,
    WEEKS,
    DAYS,
    HOURS,
    MINUTES,
    SECONDS,
    MILLISECONDS;
  }

  private TimeUnit _timeUnit = null;
  private DateTimeTransformUnit _dateTimeTransformUnit = null;

  public DateTimeFormatUnitSpec(String unit) {
    if (!isValidUnitSpec(unit)) {
      throw new IllegalArgumentException("Unit must belong to enum TimeUnit or DateTimeTransformUnit");
    }
    if (EnumUtils.isValidEnum(TimeUnit.class, unit)) {
      _timeUnit = TimeUnit.valueOf(unit);
    }
    if (EnumUtils.isValidEnum(DateTimeTransformUnit.class, unit)) {
      _dateTimeTransformUnit = DateTimeTransformUnit.valueOf(unit);
    }
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public DateTimeTransformUnit getDateTimeTransformUnit() {
    return _dateTimeTransformUnit;
  }

  public static boolean isValidUnitSpec(String unit) {
    if (EnumUtils.isValidEnum(TimeUnit.class, unit) || EnumUtils.isValidEnum(DateTimeTransformUnit.class, unit)) {
      return true;
    }
    return false;
  }
}
