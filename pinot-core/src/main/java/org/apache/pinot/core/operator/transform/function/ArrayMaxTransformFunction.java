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
 * The ArrayMaxTransformFunction class implements arrayMax function for multi-valued columns
 *
 * Sample queries:
 * SELECT COUNT(*) FROM table WHERE arrayMax(mvColumn) > 2
 * SELECT COUNT(*) FROM table GROUP BY arrayMax(mvColumn)
 * SELECT SUM(arrayMax(mvColumn)) FROM table
 */
public class ArrayMaxTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "arrayMax";

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
      throw new IllegalArgumentException("Exactly 1 argument is required for ArrayMax transform function");
    }

    // Check that the argument is a multi-valued column or transform function
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The argument of ArrayMax transform function must be a multi-valued column or a transform function");
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
      int maxRes = Integer.MIN_VALUE;
      for (int value : intValuesMV[i]) {
        maxRes = Math.max(maxRes, value);
      }
      _intValuesSV[i] = maxRes;
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
      long maxRes = Long.MIN_VALUE;
      for (long value : longValuesMV[i]) {
        maxRes = Math.max(maxRes, value);
      }
      _longValuesSV[i] = maxRes;
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
      float maxRes = Float.NEGATIVE_INFINITY;
      for (float value : floatValuesMV[i]) {
        maxRes = Math.max(maxRes, value);
      }
      _floatValuesSV[i] = maxRes;
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
      double maxRes = Double.NEGATIVE_INFINITY;
      for (double value : doubleValuesMV[i]) {
        maxRes = Math.max(maxRes, value);
      }
      _doubleValuesSV[i] = maxRes;
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
      String maxRes = null;
      for (String value : stringValuesMV[i]) {
        if (StringUtils.compare(maxRes, value) < 0) {
          maxRes = value;
        }
      }
      _stringValuesSV[i] = maxRes;
    }
    return _stringValuesSV;
  }
}
