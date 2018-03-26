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
import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.BaseDateTimeTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.DateTimeTransformerFactory;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.EpochToEpochTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.EpochToSDFTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.SDFToEpochTransformer;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.SDFToSDFTransformer;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * This class implements the date time conversion transform.
 * <ul>
 * <li>This udf should be invoked with arguments:</li>
 * <ul>
 * <li>1. Column name to convert eg. Date</li>
 * <li>2. format(as defined in {@link DateTimeFieldSpec}) of the input column. eg: 1:HOURS:EPOCH,
 * 1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd</li>
 * <li>3. format(as defined in {@link DateTimeFieldSpec}) of the output expected. eg:
 * 1:MILLISECONDS:EPOCH, 1:WEEKS:EPOCH, 1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd</li>
 * <li>4. granularity to bucket data into eg: 1:HOURS, 15:MINUTES</li>
 * </ul>
 * </ul>
 * <ul>
 * <li>End to end example:</li>
 * <ul>
 * <li>if Date column is expressed in millis, and output is expected in millis but bucketed to 15
 * minutes,</li>
 * <li>dateTimeConvert(Date, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')</li>
 * <li>if Date column is expressed in hoursSinceEpoch, and output is expected in weeksSinceEpoch
 * bucketed to weeks</li>
 * <li>dateTimeConvert(Date, '1:HOURS:EPOCH', '1:WEEKS:EPOCH', '1:WEEKS')</li>
 * </ul>
 * </ul>
 * <ul>
 * <li>Outputs:</li>
 * <ul>
 * <li>Time values converted to the desired format and bucketed to desired granularity</li>
 * </ul>
 * </ul>
 */
@NotThreadSafe
public class DateTimeConversionTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "dateTimeConvert";

  private long[] _longOutput;
  private String[] _stringOutput;
  private DataType _outputType;

  @SuppressWarnings("unchecked")
  @Override
  public <T> T transform(int length, BlockValSet... input) {
    Preconditions.checkArgument(input.length == 4, TRANSFORM_NAME + " expects four arguments");

    String[] inputDateTimeFormat = input[1].getStringValuesSV();
    String inputFormat = inputDateTimeFormat[0];
    String[] outputDateTimeFormat = input[2].getStringValuesSV();
    String outputFormat = outputDateTimeFormat[0];
    String[] outputDateTimeGranularity = input[3].getStringValuesSV();
    String outputGranularity = outputDateTimeGranularity[0];

    BaseDateTimeTransformer dateTimeTransformer =
        DateTimeTransformerFactory.getDateTimeTransformer(inputFormat, outputFormat, outputGranularity);
    if (dateTimeTransformer instanceof EpochToEpochTransformer) {
      if (_longOutput == null) {
        _longOutput = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _outputType = DataType.LONG;
      }
      ((EpochToEpochTransformer) dateTimeTransformer).transform(input[0].getLongValuesSV(), _longOutput, length);
      return (T) _longOutput;
    } else if (dateTimeTransformer instanceof EpochToSDFTransformer) {
      if (_stringOutput == null) {
        _stringOutput = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _outputType = DataType.STRING;
      }
      ((EpochToSDFTransformer) dateTimeTransformer).transform(input[0].getLongValuesSV(), _stringOutput, length);
      return (T) _stringOutput;
    } else if (dateTimeTransformer instanceof SDFToEpochTransformer) {
      if (_longOutput == null) {
        _longOutput = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _outputType = DataType.LONG;
      }
      ((SDFToEpochTransformer) dateTimeTransformer).transform(input[0].getStringValuesSV(), _longOutput, length);
      return (T) _longOutput;
    } else {
      if (_stringOutput == null) {
        _stringOutput = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _outputType = DataType.STRING;
      }
      ((SDFToSDFTransformer) dateTimeTransformer).transform(input[0].getStringValuesSV(), _stringOutput, length);
      return (T) _stringOutput;
    }
  }

  @Override
  public FieldSpec.DataType getOutputType() {
    return _outputType;
  }

  @Override
  public String getName() {
    return TRANSFORM_NAME;
  }
}
