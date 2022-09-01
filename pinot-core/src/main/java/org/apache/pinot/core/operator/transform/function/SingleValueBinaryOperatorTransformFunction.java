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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <).
 * The results are BOOLEAN type.
 */
public abstract class SingleValueBinaryOperatorTransformFunction extends BinaryOperatorTransformFunction {

  protected SingleValueBinaryOperatorTransformFunction(TransformFunctionType transformFunctionType) {
    // translate to integer in [0, 5] for guaranteed tableswitch
    switch (transformFunctionType) {
      case EQUALS:
        _op = EQUALS;
        break;
      case GREATER_THAN_OR_EQUAL:
        _op = GREATER_THAN_OR_EQUAL;
        break;
      case GREATER_THAN:
        _op = GREATER_THAN;
        break;
      case LESS_THAN:
        _op = LESS_THAN;
        break;
      case LESS_THAN_OR_EQUAL:
        _op = LESS_THAN_OR_EQUAL;
        break;
      case NOT_EQUALS:
        _op = NOT_EQUAL;
        break;
      default:
        throw new IllegalArgumentException("non-binary transform function provided: " + transformFunctionType);
    }
    _transformFunctionType = transformFunctionType;
  }

  @Override
  public String getName() {
    return _transformFunctionType.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exact 2 arguments
    Preconditions.checkArgument(arguments.size() == 2,
        "Exact 2 arguments are required for binary operator transform function");
    _leftTransformFunction = arguments.get(0);
    _rightTransformFunction = arguments.get(1);
    _leftStoredType = _leftTransformFunction.getResultMetadata().getDataType().getStoredType();
    _rightStoredType = _rightTransformFunction.getResultMetadata().getDataType().getStoredType();
    // Data type check: left and right types should be compatible.
    if (_leftStoredType == DataType.BYTES || _rightStoredType == DataType.BYTES) {
      Preconditions.checkState(_leftStoredType == _rightStoredType, String.format(
          "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform "
              + "Function [%s] result type is [%s]]", _leftTransformFunction.getName(), _leftStoredType,
          _rightTransformFunction.getName(), _rightStoredType));
    }
  }

  // The following functions will be override based on left-hand-side transform result metadata

  @Override
  protected void fillResultString(ProjectionBlock projectionBlock, int length) {
    String[] leftStringValues = _leftTransformFunction.transformToStringValuesSV(projectionBlock);
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(leftStringValues[i].compareTo(rightStringValues[i]));
    }
  }

  @Override
  protected void fillResultBytes(ProjectionBlock projectionBlock, int length) {
    byte[][] leftBytesValues = _leftTransformFunction.transformToBytesValuesSV(projectionBlock);
    byte[][] rightBytesValues = _rightTransformFunction.transformToBytesValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult((ByteArray.compare(leftBytesValues[i], rightBytesValues[i])));
    }
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    int[] leftValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Integer.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    long[] leftValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Long.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    float[] leftValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    BigDecimal[] leftValues = _leftTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightValues[i])));
    }
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    int[] leftValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Long.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    long[] leftValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Long.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    float[] leftValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    BigDecimal[] leftValues = _leftTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightValues[i])));
    }
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    int[] leftValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    long[] leftValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = compare(leftValues[i], rightValues[i]);
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    float[] leftValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Float.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    BigDecimal[] leftValues = _leftTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightValues[i])));
    }
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    int[] leftValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    long[] leftValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = compare(leftValues[i], rightValues[i]);
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    float[] leftValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(Double.compare(leftValues[i], rightValues[i]));
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    BigDecimal[] leftValues = _leftTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightValues[i])));
    }
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    int[] leftValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightValues[i]));
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    long[] leftValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightValues[i]));
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    float[] leftValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightValues[i]));
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    double[] leftValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightValues[i]));
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    BigDecimal[] leftValues = _leftTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = getIntResult(leftValues[i].compareTo(rightValues[i]));
    }
  }
}
