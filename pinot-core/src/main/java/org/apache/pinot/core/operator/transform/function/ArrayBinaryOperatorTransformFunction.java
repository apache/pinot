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


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <).
 * The results are BOOLEAN type.
 */
public abstract class ArrayBinaryOperatorTransformFunction extends BinaryOperatorTransformFunction {

  protected final TransformFunctionType _mvColumnConsolidationType;
  protected BinaryOperatorTransformFunction _binaryTransformFunction;

  protected ArrayBinaryOperatorTransformFunction(TransformFunctionType mvColumnConsolidationType) {
    _mvColumnConsolidationType = mvColumnConsolidationType;
  }

  @Override
  public String getName() {
    return _mvColumnConsolidationType.getName() + "(" + _transformFunctionType.getName() + ")";
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exact 1 arguments for the match binary functions
    Preconditions.checkArgument(arguments.size() == 1
            && arguments.get(0) instanceof BinaryOperatorTransformFunction,
        "Exact 1 binary op function argument is required for array binary operator transform function");
    // By pass the binary transform and use the multi-value + binary transform function.
    _binaryTransformFunction = (BinaryOperatorTransformFunction) arguments.get(0);
    _op = _binaryTransformFunction._op;
    _transformFunctionType = _binaryTransformFunction._transformFunctionType;
    _leftTransformFunction = _binaryTransformFunction._leftTransformFunction;
    _rightTransformFunction = _binaryTransformFunction._rightTransformFunction;
    // MV and SV column type check: left size must be MV and right side must be SV.
    Preconditions.checkState(!_leftTransformFunction.getResultMetadata().isSingleValue(),
        "Unsupported data type for comparison: [Left Transform Function [%s] result is not multi-value.");
    Preconditions.checkState(_rightTransformFunction.getResultMetadata().isSingleValue(),
        "Unsupported data type for comparison: [Right Transform Function [%s] result is not single-value.");
    // TODO: Data type check: left and right types should be compatible.
    _leftStoredType = _leftTransformFunction.getResultMetadata().getDataType().getStoredType();
    _rightStoredType = _rightTransformFunction.getResultMetadata().getDataType().getStoredType();
  }

  // The following functions will be override based on left-hand-side transform result metadata

  @Override
  protected void fillResultString(ProjectionBlock projectionBlock, int length) {
    String[][] leftStringValues = _leftTransformFunction.transformToStringValuesMV(projectionBlock);
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (String leftStringValue : leftStringValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(leftStringValue.compareTo(rightStringValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillResultBytes(ProjectionBlock projectionBlock, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    int[][] leftValues = _leftTransformFunction.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (int leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Integer.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    long[][] leftValues = _leftTransformFunction.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (long leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Long.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    float[][] leftValues = _leftTransformFunction.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (float leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    double[][] leftValues = _leftTransformFunction.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (double leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    int[][] leftValues = _leftTransformFunction.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (int leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(getIntResult(Long.compare(leftValue, rightValues[i]))));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    long[][] leftValues = _leftTransformFunction.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (long leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Long.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    float[][] leftValues = _leftTransformFunction.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (float leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    double[][] leftValues = _leftTransformFunction.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (double leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    int[][] leftValues = _leftTransformFunction.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (int leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    long[][] leftValues = _leftTransformFunction.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (long leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], compare(leftValue, rightValues[i]));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    float[][] leftValues = _leftTransformFunction.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (float leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Float.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    double[][] leftValues = _leftTransformFunction.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (double leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    int[][] leftValues = _leftTransformFunction.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (int leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    long[][] leftValues = _leftTransformFunction.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (long leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], compare(leftValue, rightValues[i]));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    float[][] leftValues = _leftTransformFunction.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (float leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    double[][] leftValues = _leftTransformFunction.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (double leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i], getIntResult(Double.compare(leftValue, rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void fillIntResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    int[][] leftValues = _leftTransformFunction.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (int leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i],
            getIntResult(BigDecimal.valueOf(leftValue).compareTo(rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillLongResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    long[][] leftValues = _leftTransformFunction.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (long leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i],
            getIntResult(BigDecimal.valueOf(leftValue).compareTo(rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillFloatResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    float[][] leftValues = _leftTransformFunction.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (float leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i],
            getIntResult(BigDecimal.valueOf(leftValue).compareTo(rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillDoubleResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    double[][] leftValues = _leftTransformFunction.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < length; i++) {
      _results[i] = initResult();
      for (double leftValue : leftValues[i]) {
        _results[i] = mergeIntResult(_results[i],
            getIntResult(BigDecimal.valueOf(leftValue).compareTo(rightValues[i])));
        if (isEarlyTerminate(_results[i])) {
          break;
        }
      }
    }
  }

  @Override
  protected void fillBigDecimalResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length) {
    throw new UnsupportedOperationException();
  }

  // Utility functions override

  protected abstract int initResult();

  protected abstract int mergeIntResult(int prev, int curr);

  protected abstract boolean isEarlyTerminate(int result);
}
