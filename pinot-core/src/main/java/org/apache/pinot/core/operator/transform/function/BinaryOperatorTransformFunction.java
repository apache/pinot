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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <).
 * The results are BOOLEAN type.
 */
public abstract class BinaryOperatorTransformFunction extends BaseTransformFunction {
  private static final int EQUALS = 0;
  private static final int GREATER_THAN_OR_EQUAL = 1;
  private static final int GREATER_THAN = 2;
  private static final int LESS_THAN = 3;
  private static final int LESS_THAN_OR_EQUAL = 4;
  private static final int NOT_EQUAL = 5;

  protected final int _op;
  protected final TransformFunctionType _transformFunctionType;
  protected TransformFunction _leftTransformFunction;
  protected TransformFunction _rightTransformFunction;
  protected DataType _leftStoredType;
  protected DataType _rightStoredType;

  protected BinaryOperatorTransformFunction(TransformFunctionType transformFunctionType) {
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
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
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

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    fillResultArray(valueBlock);
    return _intValuesSV;
  }

  private void fillResultArray(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    switch (_leftStoredType) {
      case INT:
        fillResultInt(valueBlock, length);
        break;
      case LONG:
        fillResultLong(valueBlock, length);
        break;
      case FLOAT:
        fillResultFloat(valueBlock, length);
        break;
      case DOUBLE:
        fillResultDouble(valueBlock, length);
        break;
      case BIG_DECIMAL:
        fillResultBigDecimal(valueBlock, length);
        break;
      case STRING:
        fillResultString(valueBlock, length);
        break;
      case BYTES:
        fillResultBytes(valueBlock, length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      // NOTE: Multi-value columns are not comparable, so we should not reach here
      default:
        throw illegalState();
    }
  }

  private void fillResultInt(ValueBlock valueBlock, int length) {
    int[] leftIntValues = _leftTransformFunction.transformToIntValuesSV(valueBlock);
    switch (_rightStoredType) {
      case INT:
        fillIntResultArray(valueBlock, leftIntValues, length);
        break;
      case LONG:
        fillLongResultArray(valueBlock, leftIntValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(valueBlock, leftIntValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(valueBlock, leftIntValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(valueBlock, leftIntValues, length);
        break;
      case STRING:
        fillStringResultArray(valueBlock, leftIntValues, length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultLong(ValueBlock valueBlock, int length) {
    long[] leftLongValues = _leftTransformFunction.transformToLongValuesSV(valueBlock);
    switch (_rightStoredType) {
      case INT:
        fillIntResultArray(valueBlock, leftLongValues, length);
        break;
      case LONG:
        fillLongResultArray(valueBlock, leftLongValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(valueBlock, leftLongValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(valueBlock, leftLongValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(valueBlock, leftLongValues, length);
        break;
      case STRING:
        fillStringResultArray(valueBlock, leftLongValues, length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultFloat(ValueBlock valueBlock, int length) {
    float[] leftFloatValues = _leftTransformFunction.transformToFloatValuesSV(valueBlock);
    switch (_rightStoredType) {
      case INT:
        fillIntResultArray(valueBlock, leftFloatValues, length);
        break;
      case LONG:
        fillLongResultArray(valueBlock, leftFloatValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(valueBlock, leftFloatValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(valueBlock, leftFloatValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(valueBlock, leftFloatValues, length);
        break;
      case STRING:
        fillStringResultArray(valueBlock, leftFloatValues, length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultDouble(ValueBlock valueBlock, int length) {
    double[] leftDoubleValues = _leftTransformFunction.transformToDoubleValuesSV(valueBlock);
    switch (_rightStoredType) {
      case INT:
        fillIntResultArray(valueBlock, leftDoubleValues, length);
        break;
      case LONG:
        fillLongResultArray(valueBlock, leftDoubleValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(valueBlock, leftDoubleValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(valueBlock, leftDoubleValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(valueBlock, leftDoubleValues, length);
        break;
      case STRING:
        fillStringResultArray(valueBlock, leftDoubleValues, length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultBigDecimal(ValueBlock valueBlock, int length) {
    BigDecimal[] leftBigDecimalValues = _leftTransformFunction.transformToBigDecimalValuesSV(valueBlock);
    switch (_rightStoredType) {
      case INT:
        fillIntResultArray(valueBlock, leftBigDecimalValues, length);
        break;
      case LONG:
        fillLongResultArray(valueBlock, leftBigDecimalValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(valueBlock, leftBigDecimalValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(valueBlock, leftBigDecimalValues, length);
        break;
      case STRING:
        fillStringResultArray(valueBlock, leftBigDecimalValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(valueBlock, leftBigDecimalValues, length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw illegalState();
    }
  }

  private IllegalStateException illegalState() {
    throw new IllegalStateException(String.format(
        "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right "
            + "Transform Function [%s] result type is [%s]]", _leftTransformFunction.getName(), _leftStoredType,
        _rightTransformFunction.getName(), _rightStoredType));
  }

  private void fillResultString(ValueBlock valueBlock, int length) {
    String[] leftStringValues = _leftTransformFunction.transformToStringValuesSV(valueBlock);
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftStringValues[i].compareTo(rightStringValues[i]));
    }
  }

  private void fillResultBytes(ValueBlock valueBlock, int length) {
    byte[][] leftBytesValues = _leftTransformFunction.transformToBytesValuesSV(valueBlock);
    byte[][] rightBytesValues = _rightTransformFunction.transformToBytesValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult((ByteArray.compare(leftBytesValues[i], rightBytesValues[i])));
    }
  }

  private void fillIntResultArray(ValueBlock valueBlock, int[] leftIntValues, int length) {
    int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Integer.compare(leftIntValues[i], rightIntValues[i]));
    }
  }

  private void fillLongResultArray(ValueBlock valueBlock, int[] leftValues, int length) {
    long[] rightValues = _rightTransformFunction.transformToLongValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Long.compare(leftValues[i], rightValues[i]));
    }
  }

  private void fillFloatResultArray(ValueBlock valueBlock, int[] leftValues, int length) {
    float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightFloatValues[i]));
    }
  }

  private void fillDoubleResultArray(ValueBlock valueBlock, int[] leftValues, int length) {
    double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightDoubleValues[i]));
    }
  }

  private void fillBigDecimalResultArray(ValueBlock valueBlock, int[] leftValues, int length) {
    BigDecimal[] rightBigDecimalValues = _rightTransformFunction.transformToBigDecimalValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightBigDecimalValues[i]));
    }
  }

  private void fillStringResultArray(ValueBlock valueBlock, int[] leftValues, int length) {
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      try {
        _intValuesSV[i] =
            getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
      } catch (NumberFormatException e) {
        _intValuesSV[i] = 0;
      }
    }
  }

  private void fillIntResultArray(ValueBlock valueBlock, long[] leftIntValues, int length) {
    int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Long.compare(leftIntValues[i], rightIntValues[i]));
    }
  }

  private void fillLongResultArray(ValueBlock valueBlock, long[] leftValues, int length) {
    long[] rightValues = _rightTransformFunction.transformToLongValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Long.compare(leftValues[i], rightValues[i]));
    }
  }

  private void fillFloatResultArray(ValueBlock valueBlock, long[] leftValues, int length) {
    float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(compare(leftValues[i], rightFloatValues[i]));
    }
  }

  private void fillDoubleResultArray(ValueBlock valueBlock, long[] leftValues, int length) {
    double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(compare(leftValues[i], rightDoubleValues[i]));
    }
  }

  private void fillBigDecimalResultArray(ValueBlock valueBlock, long[] leftValues, int length) {
    BigDecimal[] rightBigDecimalValues = _rightTransformFunction.transformToBigDecimalValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightBigDecimalValues[i]));
    }
  }

  private void fillStringResultArray(ValueBlock valueBlock, long[] leftValues, int length) {
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      try {
        _intValuesSV[i] =
            getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
      } catch (NumberFormatException e) {
        _intValuesSV[i] = 0;
      }
    }
  }

  private void fillIntResultArray(ValueBlock valueBlock, float[] leftValues, int length) {
    int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightIntValues[i]));
    }
  }

  private void fillLongResultArray(ValueBlock valueBlock, float[] leftValues, int length) {
    long[] rightValues = _rightTransformFunction.transformToLongValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(compare(leftValues[i], rightValues[i]));
    }
  }

  private void fillFloatResultArray(ValueBlock valueBlock, float[] leftValues, int length) {
    float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Float.compare(leftValues[i], rightFloatValues[i]));
    }
  }

  private void fillDoubleResultArray(ValueBlock valueBlock, float[] leftValues, int length) {
    double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightDoubleValues[i]));
    }
  }

  private void fillBigDecimalResultArray(ValueBlock valueBlock, float[] leftValues, int length) {
    BigDecimal[] rightBigDecimalValues = _rightTransformFunction.transformToBigDecimalValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightBigDecimalValues[i]));
    }
  }

  private void fillStringResultArray(ValueBlock valueBlock, float[] leftValues, int length) {
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      try {
        _intValuesSV[i] =
            getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
      } catch (NumberFormatException e) {
        _intValuesSV[i] = 0;
      }
    }
  }

  private void fillIntResultArray(ValueBlock valueBlock, double[] leftValues, int length) {
    int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightIntValues[i]));
    }
  }

  private void fillLongResultArray(ValueBlock valueBlock, double[] leftValues, int length) {
    long[] rightValues = _rightTransformFunction.transformToLongValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(compare(leftValues[i], rightValues[i]));
    }
  }

  private void fillFloatResultArray(ValueBlock valueBlock, double[] leftValues, int length) {
    float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightFloatValues[i]));
    }
  }

  private void fillDoubleResultArray(ValueBlock valueBlock, double[] leftValues, int length) {
    double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(Double.compare(leftValues[i], rightDoubleValues[i]));
    }
  }

  private void fillBigDecimalResultArray(ValueBlock valueBlock, double[] leftValues, int length) {
    BigDecimal[] rightBigDecimalValues = _rightTransformFunction.transformToBigDecimalValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(rightBigDecimalValues[i]));
    }
  }

  private void fillStringResultArray(ValueBlock valueBlock, double[] leftValues, int length) {
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      try {
        _intValuesSV[i] =
            getIntResult(BigDecimal.valueOf(leftValues[i]).compareTo(new BigDecimal(rightStringValues[i])));
      } catch (NumberFormatException e) {
        _intValuesSV[i] = 0;
      }
    }
  }

  private void fillIntResultArray(ValueBlock valueBlock, BigDecimal[] leftValues, int length) {
    int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightIntValues[i])));
    }
  }

  private void fillLongResultArray(ValueBlock valueBlock, BigDecimal[] leftValues, int length) {
    long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightLongValues[i])));
    }
  }

  private void fillFloatResultArray(ValueBlock valueBlock, BigDecimal[] leftValues, int length) {
    float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightFloatValues[i])));
    }
  }

  private void fillDoubleResultArray(ValueBlock valueBlock, BigDecimal[] leftValues, int length) {
    double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftValues[i].compareTo(BigDecimal.valueOf(rightDoubleValues[i])));
    }
  }

  private void fillBigDecimalResultArray(ValueBlock valueBlock, BigDecimal[] leftValues, int length) {
    BigDecimal[] rightBigDecimalValues = _rightTransformFunction.transformToBigDecimalValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftValues[i].compareTo(rightBigDecimalValues[i]));
    }
  }

  private void fillStringResultArray(ValueBlock valueBlock, BigDecimal[] leftValues, int length) {
    String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = getIntResult(leftValues[i].compareTo(new BigDecimal(rightStringValues[i])));
    }
  }

  private int compare(long left, double right) {
    if (Math.abs(left) <= 1L << 53) {
      return Double.compare(left, right);
    } else {
      return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right));
    }
  }

  private int compare(double left, long right) {
    if (Math.abs(right) <= 1L << 53) {
      return Double.compare(left, right);
    } else {
      return BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right));
    }
  }

  private int getIntResult(int comparisonResult) {
    return getBinaryFuncResult(comparisonResult) ? 1 : 0;
  }

  private boolean getBinaryFuncResult(int comparisonResult) {
    switch (_op) {
      case EQUALS:
        return comparisonResult == 0;
      case GREATER_THAN_OR_EQUAL:
        return comparisonResult >= 0;
      case GREATER_THAN:
        return comparisonResult > 0;
      case LESS_THAN:
        return comparisonResult < 0;
      case LESS_THAN_OR_EQUAL:
        return comparisonResult <= 0;
      case NOT_EQUAL:
        return comparisonResult != 0;
      default:
        throw new IllegalStateException();
    }
  }
}
