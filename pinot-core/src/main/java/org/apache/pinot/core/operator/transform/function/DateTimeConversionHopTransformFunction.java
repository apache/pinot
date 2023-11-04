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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.BaseDateTimeWindowHopTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.DateTimeWindowHopTransformerFactory;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.EpochToEpochWindowHopTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.EpochToSDFHopWindowTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.SDFToEpochWindowHopTransformer;
import org.apache.pinot.core.operator.transform.transformer.datetimehop.SDFToSDFWindowHopTransformer;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>DateTimeConversionHopTransformFunction</code> class implements the date time conversion
 * with hop transform function.
 * <ul>
 *   <li>
 *     This transform function should be invoked with arguments:
 *     <ul>
 *       <li>Column name to convert. E.g. Date</li>
 *       <li>Input format of the column. E.g. EPOCH|MILLISECONDS (See Pipe Format in DateTimeFormatSpec)</li>
 *       <li>Output format. E.g. EPOCH|MILLISECONDS/|10</li>
 *       <li>Output granularity. E.g. MINUTES|15</li>
 *       <li>Hop window size. E.g. HOURS</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Outputs:
 *     <ul>
 *       <li>Time values converted to the desired format and bucketed to desired granularity with hop windows</li>
 *       <li>Below is an example for one hour window with 15min hop for 12:10</li>
 *        |-----------------|  11:15 - 12:15
 *            |-----------------|  11:30 - 12:30
 *                |-----------------|  11:45 - 12:45
 *                    |-----------------|  12:00 - 13:00
 *       <li>The beginning of the windows returned</>
 *       <li>The end of the window can be fetched by adding window size</>
 *     </ul>
 *   </li>
 * </ul>
 */
public class DateTimeConversionHopTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "dateTimeConvertWindowHop";

  private TransformFunction _mainTransformFunction;
  private TransformResultMetadata _resultMetadata;
  private BaseDateTimeWindowHopTransformer<?, ?> _dateTimeTransformer;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there are exactly 4 arguments
    if (arguments.size() != 5) {
      throw new IllegalArgumentException("Exactly 5 arguments are required for DATE_TIME_CONVERT_HOP function");
    }
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of DATE_TIME_CONVERT_HOP transform function must be a single-valued column or "
              + "a transform function");
    }
    _mainTransformFunction = firstArgument;

    _dateTimeTransformer = DateTimeWindowHopTransformerFactory.getDateTimeTransformer(
        ((LiteralTransformFunction) arguments.get(1)).getStringLiteral(),
        ((LiteralTransformFunction) arguments.get(2)).getStringLiteral(),
        ((LiteralTransformFunction) arguments.get(3)).getStringLiteral(),
        ((LiteralTransformFunction) arguments.get(4)).getStringLiteral());
    if (_dateTimeTransformer instanceof EpochToEpochWindowHopTransformer
        || _dateTimeTransformer instanceof SDFToEpochWindowHopTransformer) {
      _resultMetadata = LONG_MV_NO_DICTIONARY_METADATA;
    } else {
      _resultMetadata = STRING_MV_NO_DICTIONARY_METADATA;
    }
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata != LONG_MV_NO_DICTIONARY_METADATA) {
      return super.transformToLongValuesMV(valueBlock);
    }

    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    if (_dateTimeTransformer instanceof EpochToEpochWindowHopTransformer) {
      EpochToEpochWindowHopTransformer dateTimeTransformer = (EpochToEpochWindowHopTransformer) _dateTimeTransformer;
      dateTimeTransformer.transform(_mainTransformFunction.transformToLongValuesSV(valueBlock), _longValuesMV, length);
    } else if (_dateTimeTransformer instanceof SDFToEpochWindowHopTransformer) {
      SDFToEpochWindowHopTransformer dateTimeTransformer = (SDFToEpochWindowHopTransformer) _dateTimeTransformer;
      dateTimeTransformer.transform(_mainTransformFunction.transformToStringValuesSV(valueBlock), _longValuesMV,
          length);
    }
    return _longValuesMV;
  }

  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata != STRING_MV_NO_DICTIONARY_METADATA) {
      return super.transformToStringValuesMV(valueBlock);
    }

    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    if (_dateTimeTransformer instanceof EpochToSDFHopWindowTransformer) {
      EpochToSDFHopWindowTransformer dateTimeTransformer = (EpochToSDFHopWindowTransformer) _dateTimeTransformer;
      dateTimeTransformer.transform(_mainTransformFunction.transformToLongValuesSV(valueBlock), _stringValuesMV,
          length);
    } else if (_dateTimeTransformer instanceof SDFToSDFWindowHopTransformer) {
      SDFToSDFWindowHopTransformer dateTimeTransformer = (SDFToSDFWindowHopTransformer) _dateTimeTransformer;
      dateTimeTransformer.transform(_mainTransformFunction.transformToStringValuesSV(valueBlock), _stringValuesMV,
          length);
    }
    return _stringValuesMV;
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return _mainTransformFunction.getNullBitmap(valueBlock);
  }
}
