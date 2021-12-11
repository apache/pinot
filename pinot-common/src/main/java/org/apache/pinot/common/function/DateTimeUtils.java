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
package org.apache.pinot.common.function;

import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods and constructs for datetrunc function
 */
public class DateTimeUtils {
  private DateTimeUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeUtils.class);
  private static final DateTimeFieldType QUARTER_OF_YEAR = new QuarterOfYearDateTimeField();

  public static DateTimeField getTimestampField(ISOChronology chronology, String unitString) {
    switch (unitString.toLowerCase()) {
      case "millisecond":
        return chronology.millisOfSecond();
      case "second":
        return chronology.secondOfMinute();
      case "minute":
        return chronology.minuteOfHour();
      case "hour":
        return chronology.hourOfDay();
      case "day":
        return chronology.dayOfMonth();
      case "week":
        return chronology.weekOfWeekyear();
      case "month":
        return chronology.monthOfYear();
      case "quarter":
        return QUARTER_OF_YEAR.getField(chronology);
      case "year":
        return chronology.year();
      default:
        throw new IllegalArgumentException("'" + unitString + "' is not a valid Timestamp field");
    }
  }

  public static ISOChronology getChronology(TimeZoneKey timeZoneKey) {
    return DateTimeZoneIndex.getChronology(timeZoneKey);
  }

  public static final class DateTimeZoneIndex {
    private static final DateTimeZone[] DATE_TIME_ZONES;
    private static final ISOChronology[] CHRONOLOGIES;
    private static final int[] FIXED_ZONE_OFFSET;
    private static final int VARIABLE_ZONE = Integer.MAX_VALUE;

    private DateTimeZoneIndex() {
    }

    public static ISOChronology getChronology(TimeZoneKey zoneKey) {
      return CHRONOLOGIES[zoneKey.getKey()];
    }

    public static DateTimeZone getDateTimeZone(TimeZoneKey zoneKey) {
      return DATE_TIME_ZONES[zoneKey.getKey()];
    }

    static {
      DATE_TIME_ZONES = new DateTimeZone[TimeZoneKey.MAX_TIME_ZONE_KEY + 1];
      CHRONOLOGIES = new ISOChronology[TimeZoneKey.MAX_TIME_ZONE_KEY + 1];
      FIXED_ZONE_OFFSET = new int[TimeZoneKey.MAX_TIME_ZONE_KEY + 1];
      for (TimeZoneKey timeZoneKey : TimeZoneKey.getTimeZoneKeys()) {
        short zoneKey = timeZoneKey.getKey();
        DateTimeZone dateTimeZone;
        try {
          dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
        } catch (IllegalArgumentException e) {
          LOGGER.error("Exception while extracting time zone field", e);
          continue;
        }
        DATE_TIME_ZONES[zoneKey] = dateTimeZone;
        CHRONOLOGIES[zoneKey] = ISOChronology.getInstance(dateTimeZone);
        if (dateTimeZone.isFixed() && dateTimeZone.getOffset(0) % 60_000 == 0) {
          FIXED_ZONE_OFFSET[zoneKey] = dateTimeZone.getOffset(0) / 60_000;
        } else {
          FIXED_ZONE_OFFSET[zoneKey] = VARIABLE_ZONE;
        }
      }
    }
  }

  // Original comment from presto code
  // ```Forked from org.elasticsearch.common.joda.Joda```
  public static final class QuarterOfYearDateTimeField extends DateTimeFieldType {
    private static final DateTimeFieldType QUARTER_OF_YEAR = new QuarterOfYearDateTimeField();
    private static final long serialVersionUID = -5677872459807379123L;
    private static final DurationFieldType QUARTER_OF_YEAR_DURATION_FIELD_TYPE = new QuarterOfYearDurationFieldType();

    private QuarterOfYearDateTimeField() {
      super("quarterOfYear");
    }

    @Override
    public DurationFieldType getDurationType() {
      return QUARTER_OF_YEAR_DURATION_FIELD_TYPE;
    }

    @Override
    public DurationFieldType getRangeDurationType() {
      return DurationFieldType.years();
    }

    @Override
    public DateTimeField getField(Chronology chronology) {
      return new OffsetDateTimeField(
          new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QUARTER_OF_YEAR, 3), 1);
    }

    private static class QuarterOfYearDurationFieldType extends DurationFieldType {
      private static final long serialVersionUID = -8167713675442491871L;

      public QuarterOfYearDurationFieldType() {
        super("quarters");
      }

      @Override
      public DurationField getField(Chronology chronology) {
        return new ScaledDurationField(chronology.months(), QUARTER_OF_YEAR_DURATION_FIELD_TYPE, 3);
      }
    }
  }
}
