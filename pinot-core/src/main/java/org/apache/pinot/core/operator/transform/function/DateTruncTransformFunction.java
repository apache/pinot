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
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.common.function.TimeZoneKey;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.joda.time.DateTimeField;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>DateTruncTransformationFunction</code> class implements the sql compatible date_trunc function for
 * TIMESTAMP type.
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
 *          Optional Truncation Time zone as specified in the zone-index.properties file, these timezones are both
 *          DateTime and JodaTime compatible.
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
 *     to_unixtime(date_trunc('hour', from_unixtime(ts_in_millis/1000.0))) * 1000 -> dateTrunc('hour', ts_in_millis,
 *     'MILLISECONDS')
 *   </li>
 *   <li>
 *     to_unixtime(date_trunc('month', from_unixtime(ts_in_seconds, 'Europe/Berlin'))) -> dateTrunc('month',
 *     ts_in_millis, 'SECONDS', 'Europe/Berlin')
 *     Note how the truncation is done at months, as defined by the Berlin timezone.
 *   </li>
 * </ul>
 */
public class DateTruncTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "dateTrunc";
  public static final String EXAMPLE_INVOCATION =
      String.format("%s('week', time_expression, 'seconds', <TZ>, <Output-Granularity>)", FUNCTION_NAME);
  private static final String UTC_TZ = TimeZoneKey.UTC_KEY.getId();
  private TransformFunction _mainTransformFunction;
  private TransformResultMetadata _resultMetadata;
  private DateTimeField _field;
  private TimeUnit _inputTimeUnit;
  private TimeUnit _outputTimeUnit;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(arguments.size() >= 2 && arguments.size() <= 5,
        "Between two to five arguments are required, example: %s", EXAMPLE_INVOCATION);
    String unit = ((LiteralTransformFunction) arguments.get(0)).getStringLiteral().toLowerCase();
    TransformFunction valueArgument = arguments.get(1);
    Preconditions.checkArgument(
        !(valueArgument instanceof LiteralTransformFunction) && valueArgument.getResultMetadata().isSingleValue(),
        "The second argument of dateTrunc transform function must be a single-valued column or a transform function");
    _mainTransformFunction = valueArgument;
    String inputTimeUnitStr =
        (arguments.size() >= 3) ? ((LiteralTransformFunction) arguments.get(2)).getStringLiteral().toUpperCase()
            : TimeUnit.MILLISECONDS.name();
    _inputTimeUnit = TimeUnit.valueOf(inputTimeUnitStr);

    String timeZone = arguments.size() >= 4 ? ((LiteralTransformFunction) arguments.get(3)).getStringLiteral() : UTC_TZ;
    String outputTimeUnitStr =
        arguments.size() >= 5 ? ((LiteralTransformFunction) arguments.get(4)).getStringLiteral().toUpperCase()
            : inputTimeUnitStr;
    TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(timeZone);

    _field = DateTimeUtils.getTimestampField(DateTimeUtils.getChronology(timeZoneKey), unit);
    _resultMetadata = LONG_SV_NO_DICTIONARY_METADATA;
    _outputTimeUnit = TimeUnit.valueOf(outputTimeUnitStr);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    long[] input = _mainTransformFunction.transformToLongValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _longValuesSV[i] =
          _outputTimeUnit.convert(_field.roundFloor(TimeUnit.MILLISECONDS.convert(input[i], _inputTimeUnit)),
              TimeUnit.MILLISECONDS);
    }
    return _longValuesSV;
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return _mainTransformFunction.getNullBitmap(valueBlock);
  }
}
