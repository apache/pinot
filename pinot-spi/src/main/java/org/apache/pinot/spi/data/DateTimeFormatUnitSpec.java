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
package org.apache.pinot.spi.data;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.EnumUtils;
import org.joda.time.DurationFieldType;
import org.joda.time.chrono.ISOChronology;


public class DateTimeFormatUnitSpec {

  /**
   * Time unit enum with range from MILLISECONDS to YEARS
   */
  public enum DateTimeTransformUnit {

    MILLISECONDS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return millisSinceEpoch;
      }
    },

    SECONDS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toSeconds(millisSinceEpoch);
      }
    },

    MINUTES {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toMinutes(millisSinceEpoch);
      }
    },

    HOURS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toHours(millisSinceEpoch);
      }
    },

    DAYS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return TimeUnit.MILLISECONDS.toDays(millisSinceEpoch);
      }
    },

    WEEKS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.weeks().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    },

    MONTHS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.months().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    },

    YEARS {
      @Override
      public long fromMillis(long millisSinceEpoch) {
        return DurationFieldType.years().getField(ISOChronology.getInstanceUTC()).getDifference(millisSinceEpoch, 0L);
      }
    };

    /**
     * Convert the given millisecond since epoch into the desired time unit.
     *
     * @param millisSinceEpoch Millisecond since epoch
     * @return Time since epoch of desired time unit
     */
    public abstract long fromMillis(long millisSinceEpoch);
  }

  public static final DateTimeFormatUnitSpec MILLISECONDS = new DateTimeFormatUnitSpec(TimeUnit.MILLISECONDS.name());

  private final TimeUnit _timeUnit;
  private final DateTimeTransformUnit _dateTimeTransformUnit;

  public DateTimeFormatUnitSpec(String unit) {
    if (EnumUtils.isValidEnum(TimeUnit.class, unit)) {
      _timeUnit = TimeUnit.valueOf(unit);
    } else {
      _timeUnit = null;
    }
    if (EnumUtils.isValidEnum(DateTimeTransformUnit.class, unit)) {
      _dateTimeTransformUnit = DateTimeTransformUnit.valueOf(unit);
    } else {
      _dateTimeTransformUnit = null;
    }
    Preconditions.checkArgument(_timeUnit != null || _dateTimeTransformUnit != null,
        "Unit must belong to enum TimeUnit or DateTimeTransformUnit, got: %s", unit);
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  public DateTimeTransformUnit getDateTimeTransformUnit() {
    return _dateTimeTransformUnit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DateTimeFormatUnitSpec that = (DateTimeFormatUnitSpec) o;
    return _timeUnit == that._timeUnit && _dateTimeTransformUnit == that._dateTimeTransformUnit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_timeUnit, _dateTimeTransformUnit);
  }
}
