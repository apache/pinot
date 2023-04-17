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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The ArrayMinTransformFunction class implements arrayMin function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE arrayMin(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY arrayMin(mvColumn)
 * SELECT SUM(arrayMin(mvColumn)) FROM table
 */
public class ArrayMinTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "arrayMin";

  private TransformFunction _argument;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    // Check that there is only 1 argument
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 argument is required for ArrayMin transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ArrayMin transform function must be a multi-valued column or a transform function");
    }
    _resultMetadata = new TransformResultMetadata(firstArgument.getResultMetadata().getDataType(), true, false);
    _argument = firstArgument;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_argument.getResultMetadata().getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    int[][] intValuesMV = _argument.transformToIntValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      int minRes = Integer.MAX_VALUE;
      for (int value : intValuesMV[i]) {
        minRes = Math.min(minRes, value);
      }
      _intValuesSV[i] = minRes;
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_argument.getResultMetadata().getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    long[][] longValuesMV = _argument.transformToLongValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      long minRes = Long.MAX_VALUE;
      for (long value : longValuesMV[i]) {
        minRes = Math.min(minRes, value);
      }
      _longValuesSV[i] = minRes;
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_argument.getResultMetadata().getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesSV(length);
    float[][] floatValuesMV = _argument.transformToFloatValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      float minRes = Float.POSITIVE_INFINITY;
      for (float value : floatValuesMV[i]) {
        minRes = Math.min(minRes, value);
      }
      _floatValuesSV[i] = minRes;
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_argument.getResultMetadata().getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    double[][] doubleValuesMV = _argument.transformToDoubleValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      double minRes = Double.POSITIVE_INFINITY;
      for (double value : doubleValuesMV[i]) {
        minRes = Math.min(minRes, value);
      }
      _doubleValuesSV[i] = minRes;
    }
    return _doubleValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_argument.getResultMetadata().getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    String[][] stringValuesMV = _argument.transformToStringValuesMV(valueBlock);
    for (int i = 0; i < length; i++) {
      String minRes = null;
      for (String value : stringValuesMV[i]) {
        if (StringUtils.compare(minRes, value) > 0) {
          minRes = value;
        }
      }
      _stringValuesSV[i] = minRes;
    }
    return _stringValuesSV;
  }
}
