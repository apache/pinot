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
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;


/**
 *  Created by liyang28 on 2019/5/22. Depending on the time granularity, the timezone conversion corresponds to the appropriate timestamp
 */
public class Truncate {
  private Period period;
  private Chronology chronology;
  private long origin;

  public Truncate(DateTimeZone dateTimeZone, long outputGranularityMillis) {
    this.period = new Period(outputGranularityMillis);
    this.chronology = ISOChronology.getInstance(dateTimeZone);
    this.origin = new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(this.chronology.getZone()).getMillis();
  }

  public long truncate(long t) {
    final int years = period.getYears();
    if (years > 0) {
      if (years > 1) {
        int y = chronology.years().getDifference(t, origin);
        y -= y % years;
        long tt = chronology.years().add(origin, y);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.years().add(tt, -years);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.year().roundFloor(t);
      }
    }

    final int months = period.getMonths();
    if (months > 0) {
      if (months > 1) {
        int m = chronology.months().getDifference(t, origin);
        m -= m % months;
        long tt = chronology.months().add(origin, m);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.months().add(tt, -months);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.monthOfYear().roundFloor(t);
      }
    }

    final int weeks = period.getWeeks();
    if (weeks > 0) {
      if (weeks > 1) {
        // align on multiples from origin
        int w = chronology.weeks().getDifference(t, origin);
        w -= w % weeks;
        long tt = chronology.weeks().add(origin, w);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.weeks().add(tt, -weeks);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.dayOfWeek().roundFloor(t);
        // default to Monday as beginning of the week
        return chronology.dayOfWeek().set(t, 1);
      }
    }

    final int days = period.getDays();
    if (days > 0) {
      if (days > 1) {
        // align on multiples from origin
        int d = chronology.days().getDifference(t, origin);
        d -= d % days;
        long tt = chronology.days().add(origin, d);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.days().add(tt, -days);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.hourOfDay().roundFloor(t);
        return chronology.hourOfDay().set(t, 0);
      }
    }

    final int hours = period.getHours();
    if (hours > 0) {
      if (hours > 1) {
        // align on multiples from origin
        long h = chronology.hours().getDifferenceAsLong(t, origin);
        h -= h % hours;
        long tt = chronology.hours().add(origin, h);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.hours().add(tt, -hours);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.minuteOfHour().roundFloor(t);
        return chronology.minuteOfHour().set(t, 0);
      }
    }

    final int minutes = period.getMinutes();
    if (minutes > 0) {
      // align on multiples from origin
      if (minutes > 1) {
        long m = chronology.minutes().getDifferenceAsLong(t, origin);
        m -= m % minutes;
        long tt = chronology.minutes().add(origin, m);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.minutes().add(tt, -minutes);
        } else {
          t = tt;
        }
        return t;
      } else {
        t = chronology.secondOfMinute().roundFloor(t);
        return chronology.secondOfMinute().set(t, 0);
      }
    }

    final int seconds = period.getSeconds();
    if (seconds > 0) {
      // align on multiples from origin
      if (seconds > 1) {
        long s = chronology.seconds().getDifferenceAsLong(t, origin);
        s -= s % seconds;
        long tt = chronology.seconds().add(origin, s);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.seconds().add(tt, -seconds);
        } else {
          t = tt;
        }
        return t;
      } else {
        return chronology.millisOfSecond().set(t, 0);
      }
    }

    final int millis = period.getMillis();
    if (millis > 0) {
      if (millis > 1) {
        long ms = chronology.millis().getDifferenceAsLong(t, origin);
        ms -= ms % millis;
        long tt = chronology.millis().add(origin, ms);
        // always round down to the previous period (for timestamps prior to origin)
        if (t < tt) {
          t = chronology.millis().add(tt, -millis);
        } else {
          t = tt;
        }
        return t;
      } else {
        return t;
      }
    }

    return t;
  }

  public long transform(String timeUnit, long millisSinceEpoch) {
    long tmp = 0;
    switch (timeUnit) {
      case "MILLISECONDS":
        tmp = millisSinceEpoch;
        break;
      case "SECONDS":
        tmp = TimeUnit.MILLISECONDS.toSeconds(millisSinceEpoch);
        break;
      case "MINUTES":
        tmp = TimeUnit.MILLISECONDS.toMinutes(millisSinceEpoch);
        break;
      case "HOURS":
        tmp = TimeUnit.MILLISECONDS.toHours(millisSinceEpoch);
        break;
      case "DAYS":
        tmp = chronology.days().getDifference(millisSinceEpoch, origin);
        break;
      case "WEEKS":
        tmp = chronology.weeks().getDifference(millisSinceEpoch, origin);
        break;
      case "MONTHS":
        tmp = chronology.months().getDifference(millisSinceEpoch, origin);
        break;
      case "YEARS":
        tmp = chronology.years().getDifference(millisSinceEpoch, origin);
        break;
      default:
        tmp = millisSinceEpoch;
    }
    return tmp;
  }
}
