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

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.transformer.datetime.BaseDateTimeTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetime.DateTimeTransformerFactory;
import org.apache.pinot.core.operator.transform.transformer.datetime.EpochToEpochTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetime.EpochToSDFTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetime.SDFToEpochTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetime.SDFToSDFTransformer;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * The <code>DateTimeConversionTransformFunction</code> class implements the date time conversion transform function.
 * <ul>
 *   <li>
 *     This transform function should be invoked with arguments:
 *     <ul>
 *       <li>
 *         Column name to convert. E.g. Date
 *       </li>
 *       <li>
 *         Format (as defined in {@link DateTimeFieldSpec}) of the input column. E.g. 1:HOURS:EPOCH;
 *         1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd
 *       </li>
 *       <li>
 *         Format (as defined in {@link DateTimeFieldSpec}) of the output. E.g. 1:MILLISECONDS:EPOCH; 1:WEEKS:EPOCH;
 *         1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd
 *       </li>
 *       <li>
 *         Granularity to bucket data into. E.g. 1:HOURS; 15:MINUTES
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     End to end example:
 *     <ul>
 *       <li>
 *         If Date column is expressed in millis, and output is expected in millis but bucketed to 15 minutes:
 *         dateTimeConvert(Date, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')
 *       </li>
 *       <li>
 *         If Date column is expressed in hoursSinceEpoch, and output is expected in weeksSinceEpoch bucketed to
 *         weeks: dateTimeConvert(Date, '1:HOURS:EPOCH', '1:WEEKS:EPOCH', '1:WEEKS')
 *       </li>
 *     </ul>
 *   </li>
 *   <li>
 *     Outputs:
 *     <ul>
 *       <li>
 *         Time values converted to the desired format and bucketed to desired granularity
 *       </li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class DateTimeConversionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "dateTimeConvert";

  private TransformFunction _mainTransformFunction;
  private BaseDateTimeTransformer _dateTimeTransformer;
  private TransformResultMetadata _resultMetadata;
  private long[] _longOutputTimes;
  private String[] _stringOutputTimes;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 4 arguments
    if (arguments.size() != 4) {
      throw new IllegalArgumentException("Exactly 4 arguments are required for DATE_TIME_CONVERT transform function");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of DATE_TIME_CONVERT transform function must be a single-valued column or a transform function");
    }
    _mainTransformFunction = firstArgument;

    _dateTimeTransformer =
        DateTimeTransformerFactory.getDateTimeTransformer(((LiteralTransformFunction) arguments.get(1)).getLiteral(),
            ((LiteralTransformFunction) arguments.get(2)).getLiteral(),
            ((LiteralTransformFunction) arguments.get(3)).getLiteral());
    if (_dateTimeTransformer instanceof EpochToEpochTransformer
        || _dateTimeTransformer instanceof SDFToEpochTransformer) {
      _resultMetadata = LONG_SV_NO_DICTIONARY_METADATA;
    } else {
      _resultMetadata = STRING_SV_NO_DICTIONARY_METADATA;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata == LONG_SV_NO_DICTIONARY_METADATA) {
      if (_longOutputTimes == null) {
        _longOutputTimes = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      }

      int length = projectionBlock.getNumDocs();
      if (_dateTimeTransformer instanceof EpochToEpochTransformer) {
        EpochToEpochTransformer dateTimeTransformer = (EpochToEpochTransformer) _dateTimeTransformer;
        dateTimeTransformer.transform(_mainTransformFunction.transformToLongValuesSV(projectionBlock), _longOutputTimes,
            length);
      } else {
        SDFToEpochTransformer dateTimeTransformer = (SDFToEpochTransformer) _dateTimeTransformer;
        dateTimeTransformer.transform(_mainTransformFunction.transformToStringValuesSV(projectionBlock),
            _longOutputTimes, length);
      }
      return _longOutputTimes;
    } else {
      return super.transformToLongValuesSV(projectionBlock);
    }
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata == STRING_SV_NO_DICTIONARY_METADATA) {
      if (_stringOutputTimes == null) {
        _stringOutputTimes = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      }

      int length = projectionBlock.getNumDocs();
      if (_dateTimeTransformer instanceof EpochToSDFTransformer) {
        EpochToSDFTransformer dateTimeTransformer = (EpochToSDFTransformer) _dateTimeTransformer;
        dateTimeTransformer.transform(_mainTransformFunction.transformToLongValuesSV(projectionBlock),
            _stringOutputTimes, length);
      } else {
        SDFToSDFTransformer dateTimeTransformer = (SDFToSDFTransformer) _dateTimeTransformer;
        dateTimeTransformer.transform(_mainTransformFunction.transformToStringValuesSV(projectionBlock),
            _stringOutputTimes, length);
      }
      return _stringOutputTimes;
    } else {
      return super.transformToStringValuesSV(projectionBlock);
    }
  }
}
