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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
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
 * The <id>DateTruncTransformationFunction</id> class implements the sql compatible date_trunc function for TIMESTAMP type.
 * <p>
 *  <ul>
 *    <li>
 *      This transform function should be invoked with arguments:
 *      <ul>
 *        <li>
 *        <li>
 *          Unit of truncation: second, minute, hour, day, week, month, quarter, year
 *        </li>
 *          Timestamp column input (or expression), this should always be in the specified units since epoch UTC
 *        </li>
 *        <li>
 *          Incoming units in terms of the TimeUnit enum. For example NANOSECONDS, SECONDS etc
 *        </li>
 *        <li>
 *          Optional Truncation Time zone as specified in the zone-index.properties file, these timezones are both DateTime and JodaTime compatible.
 *          If unspecified this is UTC
 *        </li>
 *        <li>
 *          Output TimeUnit enum: NANOSECONDS, SECONDS, MILLISECONDS. If unspecified this is the same as incoming unit
 *        </li>
 *      </ul>
 *    </li>
 *  </ul>
 *
 * It returns the time as output time unit since UTC epoch
 *
 * Example conversions from the presto's date_trunc invocations to the equivalent invocation.
 *
 * Note that presto has a proper TIMESTAMP (and TIMESTAMP_WITH_TIMEZONE) type. Pinot lacks such a type
 * and thus we need to specify the input timezone and granularity. Also note that Presto internally stores the timestamp
 * as milliseconds since UTC epoch.
 *
 * <ul>
 *   <li>
 *     to_unixtime(date_trunc('hour', from_unixtime(ts_in_millis/1000.0))) * 1000 -> dateTrunc('hour', ts_in_millis, 'MILLISECONDS')
 *   </li>
 *   <li>
 *     to_unixtime(date_trunc('month', from_unixtime(ts_in_seconds, 'Europe/Berlin'))) -> dateTrunc('month', ts_in_millis, 'SECONDS', 'Europe/Berlin')
 *     Note how the truncation is done at months, as defined by the Berlin timezone.
 *   </li>
 * </ul>
 */
public class DateTruncTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "dateTrunc";
  public static final String EXAMPLE_INVOCATION =
      String.format("%s('week', time_expression, 'seconds', <TZ>, <Output-Granularity>)", FUNCTION_NAME);
  private static final String UTC_TZ = TimeZoneKey.UTC_KEY.getId();
  private static final DateTimeFieldType QUARTER_OF_YEAR = new QuarterOfYearDateTimeField();
  private static final Logger LOGGER = LoggerFactory.getLogger(DateTruncTransformFunction.class);
  private TransformFunction _mainTransformFunction;
  private TransformResultMetadata _resultMetadata;
  private long[] _longOutputTimes;
  private DateTimeField _field;
  private TimeUnit _inputTimeUnit;
  private TimeUnit _outputTimeUnit;

  private static DateTimeField getTimestampField(ISOChronology chronology, String unitString) {
    switch (unitString) {
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
    }
    throw new IllegalArgumentException("'" + unitString + "' is not a valid Timestamp field");
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() >= 3 && arguments.size() <= 5,
        "Between three to five arguments are required, example: %s", EXAMPLE_INVOCATION);
    String unit = ((LiteralTransformFunction) arguments.get(0)).getLiteral().toLowerCase();
    TransformFunction valueArgument = arguments.get(1);
    Preconditions.checkArgument(
        !(valueArgument instanceof LiteralTransformFunction) && valueArgument.getResultMetadata().isSingleValue(),
        "The second argument of dateTrunc transform function must be a single-valued column or a transform function");
    _mainTransformFunction = valueArgument;
    String inputTimeUnitStr = ((LiteralTransformFunction) arguments.get(2)).getLiteral().toUpperCase();
    _inputTimeUnit = TimeUnit.valueOf(inputTimeUnitStr);

    String timeZone = arguments.size() >= 4 ? ((LiteralTransformFunction) arguments.get(3)).getLiteral() : UTC_TZ;
    String outputTimeUnitStr =
        arguments.size() >= 5 ? ((LiteralTransformFunction) arguments.get(4)).getLiteral().toUpperCase()
            : inputTimeUnitStr;
    TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(timeZone);

    _field = getTimestampField(DateTimeZoneIndex.getChronology(timeZoneKey), unit);
    _resultMetadata = LONG_SV_NO_DICTIONARY_METADATA;
    _outputTimeUnit = TimeUnit.valueOf(outputTimeUnitStr);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_longOutputTimes == null) {
      _longOutputTimes = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    long[] input = _mainTransformFunction.transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _longOutputTimes[i] = _outputTimeUnit
          .convert(_field.roundFloor(TimeUnit.MILLISECONDS.convert(input[i], _inputTimeUnit)), TimeUnit.MILLISECONDS);
    }
    return _longOutputTimes;
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

  // Original comment from presto id
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
