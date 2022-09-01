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

import java.math.BigDecimal;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <).
 * The results are BOOLEAN type.
 */
public abstract class BinaryOperatorTransformFunction extends BaseTransformFunction {

  protected static final int EQUALS = 0;
  protected static final int GREATER_THAN_OR_EQUAL = 1;
  protected static final int GREATER_THAN = 2;
  protected static final int LESS_THAN = 3;
  protected static final int LESS_THAN_OR_EQUAL = 4;
  protected static final int NOT_EQUAL = 5;

  protected int _op;
  protected TransformFunctionType _transformFunctionType;
  protected TransformFunction _leftTransformFunction;
  protected TransformFunction _rightTransformFunction;
  protected DataType _leftStoredType;
  protected DataType _rightStoredType;
  protected int[] _results;

  @Override
  public String getName() {
    return _transformFunctionType.getName();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    fillResultArray(projectionBlock);
    return _results;
  }

  private void fillResultArray(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_results == null || _results.length < length) {
      _results = new int[length];
    }
    if (_leftStoredType.isNumeric()) {
      // Numeric value requires type casting implicitly
      switch (_rightStoredType) {
        case INT:
          fillResultInt(projectionBlock, length);
          break;
        case LONG:
          fillResultLong(projectionBlock, length);
          break;
        case FLOAT:
          fillResultFloat(projectionBlock, length);
          break;
        case DOUBLE:
          fillResultDouble(projectionBlock, length);
          break;
        case BIG_DECIMAL:
          fillResultBigDecimal(projectionBlock, length);
          break;
        // NOTE: Multi-value columns are not comparable, so we should not reach here
        default:
          throw illegalState();
      }
    } else {
      // for all other types, no implicit type casting is allowed, and uses left-hand side for type determination
      // because right-hand side could have implicitly cast literals.
      switch (_leftStoredType) {
        case STRING:
          fillResultString(projectionBlock, length);
          break;
        case BYTES:
          fillResultBytes(projectionBlock, length);
          break;
        default:
          break;
      }
    }
  }

  private void fillResultInt(ProjectionBlock projectionBlock, int length) {
    int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
    switch (_leftStoredType) {
      case INT:
        fillIntResultArray(projectionBlock, rightIntValues, length);
        break;
      case LONG:
        fillLongResultArray(projectionBlock, rightIntValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(projectionBlock, rightIntValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(projectionBlock, rightIntValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(projectionBlock, rightIntValues, length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultLong(ProjectionBlock projectionBlock, int length) {
    long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
    switch (_leftStoredType) {
      case INT:
        fillIntResultArray(projectionBlock, rightLongValues, length);
        break;
      case LONG:
        fillLongResultArray(projectionBlock, rightLongValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(projectionBlock, rightLongValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(projectionBlock, rightLongValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(projectionBlock, rightLongValues, length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultFloat(ProjectionBlock projectionBlock, int length) {
    float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
    switch (_rightStoredType) {
      case INT:
        fillIntResultArray(projectionBlock, rightFloatValues, length);
        break;
      case LONG:
        fillLongResultArray(projectionBlock, rightFloatValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(projectionBlock, rightFloatValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(projectionBlock, rightFloatValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(projectionBlock, rightFloatValues, length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultDouble(ProjectionBlock projectionBlock, int length) {
    double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
    switch (_leftStoredType) {
      case INT:
        fillIntResultArray(projectionBlock, rightDoubleValues, length);
        break;
      case LONG:
        fillLongResultArray(projectionBlock, rightDoubleValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(projectionBlock, rightDoubleValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(projectionBlock, rightDoubleValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(projectionBlock, rightDoubleValues, length);
        break;
      default:
        throw illegalState();
    }
  }

  private void fillResultBigDecimal(ProjectionBlock projectionBlock, int length) {
    BigDecimal[] rightBigDecimalValues = _rightTransformFunction.transformToBigDecimalValuesSV(projectionBlock);
    switch (_leftStoredType) {
      case INT:
        fillIntResultArray(projectionBlock, rightBigDecimalValues, length);
        break;
      case LONG:
        fillLongResultArray(projectionBlock, rightBigDecimalValues, length);
        break;
      case FLOAT:
        fillFloatResultArray(projectionBlock, rightBigDecimalValues, length);
        break;
      case DOUBLE:
        fillDoubleResultArray(projectionBlock, rightBigDecimalValues, length);
        break;
      case BIG_DECIMAL:
        fillBigDecimalResultArray(projectionBlock, rightBigDecimalValues, length);
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

  // The following functions will be override based on left-hand-side transform result metadata

  protected abstract void fillResultString(ProjectionBlock projectionBlock, int length);

  protected abstract void fillResultBytes(ProjectionBlock projectionBlock, int length);

  protected abstract void fillIntResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length);

  protected abstract void fillLongResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length);

  protected abstract void fillFloatResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length);

  protected abstract void fillDoubleResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length);

  protected abstract void fillBigDecimalResultArray(ProjectionBlock projectionBlock, int[] rightValues, int length);

  protected abstract void fillIntResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length);

  protected abstract void fillLongResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length);

  protected abstract void fillFloatResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length);

  protected abstract void fillDoubleResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length);

  protected abstract void fillBigDecimalResultArray(ProjectionBlock projectionBlock, long[] rightValues, int length);

  protected abstract void fillIntResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length);

  protected abstract void fillLongResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length);

  protected abstract void fillFloatResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length);

  protected abstract void fillDoubleResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length);

  protected abstract void fillBigDecimalResultArray(ProjectionBlock projectionBlock, float[] rightValues, int length);

  protected abstract void fillIntResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length);

  protected abstract void fillLongResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length);

  protected abstract void fillFloatResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length);

  protected abstract void fillDoubleResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length);

  protected abstract void fillBigDecimalResultArray(ProjectionBlock projectionBlock, double[] rightValues, int length);

  protected abstract void fillIntResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length);

  protected abstract void fillLongResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length);

  protected abstract void fillFloatResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length);

  protected abstract void fillDoubleResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues, int length);

  protected abstract void fillBigDecimalResultArray(ProjectionBlock projectionBlock, BigDecimal[] rightValues,
      int length);

  // Utility functions

  protected int compare(long left, double right) {
    if (Math.abs(left) <= 1L << 53) {
      return getIntResult(Double.compare(left, right));
    } else {
      return getIntResult(BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)));
    }
  }

  protected int compare(double left, long right) {
    if (Math.abs(right) <= 1L << 53) {
      return getIntResult(Double.compare(left, right));
    } else {
      return getIntResult(BigDecimal.valueOf(left).compareTo(BigDecimal.valueOf(right)));
    }
  }

  protected int getIntResult(int comparisonResult) {
    return getBinaryFuncResult(comparisonResult) ? 1 : 0;
  }

  protected boolean getBinaryFuncResult(int comparisonResult) {
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
