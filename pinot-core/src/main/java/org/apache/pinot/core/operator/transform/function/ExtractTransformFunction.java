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
import java.util.stream.IntStream;
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.roaringbitmap.RoaringBitmap;


public class ExtractTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "extract";
  private TransformFunction _mainTransformFunction;
  private DateTimeUtils.ExtractFieldType _field;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for EXTRACT transform function");
    }

    _field = DateTimeUtils.ExtractFieldType.valueOf(((LiteralTransformFunction) arguments.get(0)).getStringLiteral());
    _mainTransformFunction = arguments.get(1);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    long[] timestamps = _mainTransformFunction.transformToLongValuesSV(valueBlock);
    IntStream.range(0, numDocs).forEach(i -> _intValuesSV[i] = DateTimeUtils.extract(_field, timestamps[i]));
    return _intValuesSV;
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return _mainTransformFunction.getNullBitmap(valueBlock);
  }
}
