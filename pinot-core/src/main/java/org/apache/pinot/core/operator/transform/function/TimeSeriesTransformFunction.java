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

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Usage:
 * <pre>
 *   args: time column/expression, time-unit, first time bucket value, bucket size in seconds, offset in seconds
 *   timeSeriesBucketIndex(secondsSinceEpoch, 'MILLISECONDS', 123, 10, 0)
 * </pre>
 */
public class TimeSeriesTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "timeSeriesBucketIndex";
  private TimeUnit _timeUnit;
  private long _reference = -1;
  private long _divisor = -1;
  private long _offset = 0;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    _timeUnit = TimeUnit.valueOf(((LiteralTransformFunction) arguments.get(1)).getStringLiteral().toUpperCase(
        Locale.ENGLISH));
    final long startSeconds = ((LiteralTransformFunction) arguments.get(2)).getLongLiteral();
    final long bucketSizeSeconds = ((LiteralTransformFunction) arguments.get(3)).getLongLiteral();
    _offset = _timeUnit.convert(Duration.ofSeconds(
        ((LiteralTransformFunction) arguments.get(4)).getLongLiteral()));
    _reference = _timeUnit.convert(Duration.ofSeconds(startSeconds - bucketSizeSeconds));
    _divisor = _timeUnit.convert(Duration.ofSeconds(bucketSizeSeconds));
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    long[] inputValues = _arguments.get(0).transformToLongValuesSV(valueBlock);
    for (int docIndex = 0; docIndex < length; docIndex++) {
      _intValuesSV[docIndex] = (int) (((inputValues[docIndex] + _offset) - _reference - 1) / _divisor);
    }
    return _intValuesSV;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(DataType.INT, true, false);
  }
}
