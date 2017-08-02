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
package com.linkedin.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.transform.function.time.converter.TimeConverterFactory;
import com.linkedin.pinot.core.operator.transform.function.time.converter.TimeUnitConverter;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * This class implements the time conversion transform.
 * <ul>
 *   <li> Inputs: </li>
 *   <ul>
 *     <li> Input time values </li>
 *     <li> Input time unit as defined in {@link TimeUnit} </li>
 *     <li> Desired output time unit as defined in {@link TimeUnit} </li>
 *   </ul>
 * </ul>
 *
 * <ul>
 *   <li> Outputs: </li>
 *   <ul>
 *     <li> Time values converted into desired time unit. </li>
 *   </ul>
 * </ul>
 */
@NotThreadSafe
public class TimeConversionTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "timeConvert";
  private long[] _output = null;

  @Override
  public <T> T transform(int length, BlockValSet... input) {
    Preconditions.checkArgument(input.length == 3, TRANSFORM_NAME + " expects three arguments");

    long[] inputTime = input[0].getLongValuesSV();
    String[] inputTimeUnits = input[1].getStringValuesSV();
    String[] outputTimeUnits = input[2].getStringValuesSV();

    Preconditions.checkState(inputTime.length >= length && inputTimeUnits.length >= length,
        outputTimeUnits.length >= length);

    TimeUnit inputTimeUnit = getTimeUnit(inputTimeUnits[0]);
    TimeUnitConverter converter = TimeConverterFactory.getTimeConverter(outputTimeUnits[0]);

    if (_output == null || _output.length < length) {
      _output = new long[Math.max(length, DocIdSetPlanNode.MAX_DOC_PER_CALL)];
    }
    converter.convert(inputTime, inputTimeUnit, length, _output);
    return (T) _output;
  }

  /**
   * Helper method to get TimeUnit enum from string name.
   *
   * @param timeUnitName String name of time unit.
   * @return TimeUnit enum value
   */
  private TimeUnit getTimeUnit(String timeUnitName) {
    try {
      return TimeUnit.valueOf(timeUnitName);
    } catch (IllegalArgumentException e) {
      throw new BadQueryRequestException("Illegal input time unit specified for timeConvert UDF: '" + timeUnitName
          + "', only values defined in java.util.concurrent.TimeUnit supported currently.");
    }
  }

  @Override
  public FieldSpec.DataType getOutputType() {
    return FieldSpec.DataType.LONG;
  }

  @Override
  public String getName() {
    return TRANSFORM_NAME;
  }
}
