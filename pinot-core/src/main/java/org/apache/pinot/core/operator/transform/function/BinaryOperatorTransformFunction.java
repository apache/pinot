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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.optimizer.filter.NumericalFilterOptimizer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <).
 * The results are BOOLEAN type.
 *
 * TODO: Support MV columns
 */
public abstract class BinaryOperatorTransformFunction extends BaseTransformFunction {
  private static final int EQUALS = 0;
  private static final int GREATER_THAN_OR_EQUAL = 1;
  private static final int GREATER_THAN = 2;
  private static final int LESS_THAN = 3;
  private static final int LESS_THAN_OR_EQUAL = 4;
  private static final int NOT_EQUAL = 5;

  private static final ExpressionContext LHS_PLACEHOLDER = ExpressionContext.forIdentifier("lhs");

  protected final int _op;
  protected final TransformFunctionType _transformFunctionType;
  protected TransformFunction _leftTransformFunction;
  protected TransformFunction _rightTransformFunction;
  protected DataType _leftStoredType;
  protected DataType _rightStoredType;
  protected PredicateEvaluator _predicateEvaluator;
  protected boolean _alwaysTrue;
  protected boolean _alwaysFalse;
  protected boolean _alwaysNull;

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
    DataType leftDataType = _leftTransformFunction.getResultMetadata().getDataType();
    _leftStoredType = leftDataType.getStoredType();
    _rightStoredType = _rightTransformFunction.getResultMetadata().getDataType().getStoredType();

    // Data type check: left and right types should be compatible.
    if (_leftStoredType == DataType.BYTES || _rightStoredType == DataType.BYTES) {
      Preconditions.checkState(_leftStoredType == _rightStoredType, String.format(
          "Unsupported data type for comparison: [Left Transform Function [%s] result type is [%s], Right Transform "
              + "Function [%s] result type is [%s]]", _leftTransformFunction.getName(), _leftStoredType,
          _rightTransformFunction.getName(), _rightStoredType));
    }

    // Create predicate evaluator when the right side is a literal
    if (_rightTransformFunction instanceof LiteralTransformFunction) {
      _predicateEvaluator = createPredicateEvaluator(leftDataType, _leftTransformFunction.getDictionary(),
          ((LiteralTransformFunction) _rightTransformFunction).getLiteralContext());
    }
  }

  /**
   * Creates a predicate evaluator for the binary operator. Returns {@code null} when the binary operator always
   * evaluates to the same value (true/false/null) where the predicate evaluator is not needed.
   */
  @Nullable
  private PredicateEvaluator createPredicateEvaluator(DataType leftDataType, @Nullable Dictionary leftDictionary,
      LiteralContext rightLiteral) {
    if (rightLiteral.isNull()) {
      _alwaysNull = true;
      return null;
    }
    Predicate predicate = createPredicate(leftDataType, rightLiteral);
    if (predicate == null) {
      return null;
    }
    return PredicateEvaluatorProvider.getPredicateEvaluator(predicate, leftDictionary, leftDataType);
  }

  /**
   * Creates a predicate for the binary operator. Returns {@code null} when the binary operator always evaluates to the
   * same value (true/false/null) where the predicate is not needed.
   *
   * It might rewrite the right value similar to {@link NumericalFilterOptimizer}.
   * TODO: Extract the common logic.
   */
  @Nullable
  private Predicate createPredicate(DataType leftDataType, LiteralContext rightLiteral) {
    DataType rightDataType = rightLiteral.getType();
    if (!leftDataType.isNumeric()) {
      return createPredicate(_op, rightLiteral.getStringValue(), rightDataType);
    }
    switch (rightDataType) {
      case LONG: {
        long actual = rightLiteral.getLongValue();
        switch (leftDataType) {
          case INT: {
            int converted = (int) actual;
            int comparison = Long.compare(actual, converted);
            // Set converted value to boundary value if overflow
            if (comparison != 0) {
              converted = comparison > 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            }
            return createIntPredicate(converted, comparison);
          }
          case FLOAT: {
            float converted = (float) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));
            return createFloatPredicate(converted, comparison);
          }
          case DOUBLE: {
            double converted = (double) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));
            return createDoublePredicate(converted, comparison);
          }
          default:
            return createPredicate(_op, Long.toString(actual), DataType.LONG);
        }
      }
      case FLOAT: {
        float actual = rightLiteral.getFloatValue();
        switch (leftDataType) {
          case INT: {
            int converted = (int) actual;
            int comparison = Double.compare(actual, converted);
            return createIntPredicate(converted, comparison);
          }
          case LONG: {
            long converted = (long) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));
            return createLongPredicate(converted, comparison);
          }
          default:
            return createPredicate(_op, Float.toString(actual), DataType.FLOAT);
        }
      }
      case DOUBLE: {
        double actual = rightLiteral.getDoubleValue();
        switch (leftDataType) {
          case INT: {
            int converted = (int) actual;
            int comparison = Double.compare(actual, converted);
            return createIntPredicate(converted, comparison);
          }
          case LONG: {
            long converted = (long) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));
            return createLongPredicate(converted, comparison);
          }
          case FLOAT: {
            float converted = (float) actual;
            int comparison = Double.compare(actual, converted);
            return createFloatPredicate(converted, comparison);
          }
          default:
            return createPredicate(_op, Double.toString(actual), DataType.DOUBLE);
        }
      }
      case BIG_DECIMAL:
      case STRING: {
        BigDecimal actual = rightLiteral.getBigDecimalValue();
        switch (leftDataType) {
          case INT: {
            int converted = actual.intValue();
            int comparison = actual.compareTo(BigDecimal.valueOf(converted));
            // Set converted value to boundary value if overflow
            if (comparison != 0) {
              if (actual.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                converted = Integer.MAX_VALUE;
              } else if (actual.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
                converted = Integer.MIN_VALUE;
              }
            }
            return createIntPredicate(converted, comparison);
          }
          case LONG: {
            long converted = actual.longValue();
            int comparison = actual.compareTo(BigDecimal.valueOf(converted));
            // Set converted value to boundary value if overflow
            if (comparison != 0) {
              if (actual.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                converted = Long.MAX_VALUE;
              } else if (actual.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0) {
                converted = Long.MIN_VALUE;
              }
            }
            return createLongPredicate(converted, comparison);
          }
          case FLOAT: {
            float converted = actual.floatValue();
            int comparison = actual.compareTo(BigDecimal.valueOf(converted));
            return createFloatPredicate(converted, comparison);
          }
          case DOUBLE: {
            double converted = actual.doubleValue();
            int comparison = actual.compareTo(BigDecimal.valueOf(converted));
            return createDoublePredicate(converted, comparison);
          }
          default:
            return createPredicate(_op, actual.toString(), DataType.BIG_DECIMAL);
        }
      }
      default:
        return createPredicate(_op, rightLiteral.getStringValue(), rightDataType);
    }
  }

  private Predicate createPredicate(int operator, String value, DataType dataType) {
    switch (operator) {
      case EQUALS:
        return new EqPredicate(LHS_PLACEHOLDER, value);
      case NOT_EQUAL:
        return new NotEqPredicate(LHS_PLACEHOLDER, value);
      case GREATER_THAN:
        return new RangePredicate(LHS_PLACEHOLDER, false, value, false, RangePredicate.UNBOUNDED, dataType);
      case GREATER_THAN_OR_EQUAL:
        return new RangePredicate(LHS_PLACEHOLDER, true, value, false, RangePredicate.UNBOUNDED, dataType);
      case LESS_THAN:
        return new RangePredicate(LHS_PLACEHOLDER, false, RangePredicate.UNBOUNDED, false, value, dataType);
      case LESS_THAN_OR_EQUAL:
        return new RangePredicate(LHS_PLACEHOLDER, false, RangePredicate.UNBOUNDED, true, value, dataType);
      default:
        throw new IllegalStateException();
    }
  }

  @Nullable
  private Predicate createIntPredicate(int converted, int comparison) {
    if (comparison == 0) {
      return createPredicate(_op, Integer.toString(converted), DataType.INT);
    }
    switch (_op) {
      case EQUALS:
        _alwaysFalse = true;
        return null;
      case NOT_EQUAL:
        _alwaysTrue = true;
        return null;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        if (comparison > 0 && converted == Integer.MAX_VALUE) {
          // col > Integer.MAX_VALUE
          _alwaysFalse = true;
          return null;
        } else if (comparison < 0 && converted == Integer.MIN_VALUE) {
          // col >= Integer.MIN_VALUE
          _alwaysTrue = true;
          return null;
        } else {
          return createPredicate(getOperator(comparison), Integer.toString(converted), DataType.INT);
        }
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        if (comparison > 0 && converted == Integer.MAX_VALUE) {
          // col <= Integer.MAX_VALUE
          _alwaysTrue = true;
          return null;
        } else if (comparison < 0 && converted == Integer.MIN_VALUE) {
          // col < Integer.MIN_VALUE
          _alwaysFalse = true;
          return null;
        } else {
          return createPredicate(getOperator(comparison), Integer.toString(converted), DataType.INT);
        }
      default:
        throw new IllegalStateException();
    }
  }

  @Nullable
  private Predicate createLongPredicate(long converted, int comparison) {
    if (comparison == 0) {
      return createPredicate(_op, Long.toString(converted), DataType.LONG);
    }
    switch (_op) {
      case EQUALS:
        _alwaysFalse = true;
        return null;
      case NOT_EQUAL:
        _alwaysTrue = true;
        return null;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        if (comparison > 0 && converted == Long.MAX_VALUE) {
          // col > Long.MAX_VALUE
          _alwaysFalse = true;
          return null;
        } else if (comparison < 0 && converted == Long.MIN_VALUE) {
          // col >= Long.MIN_VALUE
          _alwaysTrue = true;
          return null;
        } else {
          return createPredicate(getOperator(comparison), Long.toString(converted), DataType.LONG);
        }
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        if (comparison > 0 && converted == Long.MAX_VALUE) {
          // col <= Long.MAX_VALUE
          _alwaysTrue = true;
          return null;
        } else if (comparison < 0 && converted == Long.MIN_VALUE) {
          // col < Long.MIN_VALUE
          _alwaysFalse = true;
          return null;
        } else {
          return createPredicate(getOperator(comparison), Long.toString(converted), DataType.LONG);
        }
      default:
        throw new IllegalStateException();
    }
  }

  @Nullable
  private Predicate createFloatPredicate(float converted, int comparison) {
    if (comparison == 0) {
      return createPredicate(_op, Float.toString(converted), DataType.FLOAT);
    }
    switch (_op) {
      case EQUALS:
        _alwaysFalse = true;
        return null;
      case NOT_EQUAL:
        _alwaysTrue = true;
        return null;
      default:
        return createPredicate(getOperator(comparison), Float.toString(converted), DataType.FLOAT);
    }
  }

  @Nullable
  private Predicate createDoublePredicate(double converted, int comparison) {
    if (comparison == 0) {
      return createPredicate(_op, Double.toString(converted), DataType.DOUBLE);
    }
    switch (_op) {
      case EQUALS:
        _alwaysFalse = true;
        return null;
      case NOT_EQUAL:
        _alwaysTrue = true;
        return null;
      default:
        return createPredicate(getOperator(comparison), Double.toString(converted), DataType.DOUBLE);
    }
  }

  /**
   * Returns the operator (int value) for the given comparison result of actual value and converted value.
   */
  private int getOperator(int comparison) {
    assert comparison != 0;
    if (comparison > 0) {
      // Actual value greater than converted value
      // col >= actual -> col >  converted
      // col <  actual -> col <= converted
      if (_op == GREATER_THAN_OR_EQUAL) {
        return GREATER_THAN;
      }
      if (_op == LESS_THAN) {
        return LESS_THAN_OR_EQUAL;
      }
    } else {
      // Actual value less than converted value
      // col >  actual -> col >= converted
      // col <= actual -> col <  converted
      if (_op == GREATER_THAN) {
        return GREATER_THAN_OR_EQUAL;
      }
      if (_op == LESS_THAN_OR_EQUAL) {
        return LESS_THAN;
      }
    }
    return _op;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    if (_alwaysTrue) {
      Arrays.fill(_intValuesSV, 0, length, 1);
      return _intValuesSV;
    }
    if (_alwaysFalse || _alwaysNull) {
      Arrays.fill(_intValuesSV, 0, length, 0);
      return _intValuesSV;
    }
    if (_predicateEvaluator != null) {
      if (_predicateEvaluator.isDictionaryBased()) {
        int[] dictIds = _leftTransformFunction.transformToDictIdsSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _predicateEvaluator.applySV(dictIds[i]) ? 1 : 0;
        }
      } else {
        switch (_leftStoredType) {
          case INT:
            int[] intValues = _leftTransformFunction.transformToIntValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(intValues[i]) ? 1 : 0;
            }
            break;
          case LONG:
            long[] longValues = _leftTransformFunction.transformToLongValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(longValues[i]) ? 1 : 0;
            }
            break;
          case FLOAT:
            float[] floatValues = _leftTransformFunction.transformToFloatValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(floatValues[i]) ? 1 : 0;
            }
            break;
          case DOUBLE:
            double[] doubleValues = _leftTransformFunction.transformToDoubleValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(doubleValues[i]) ? 1 : 0;
            }
            break;
          case BIG_DECIMAL:
            BigDecimal[] bigDecimalValues = _leftTransformFunction.transformToBigDecimalValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(bigDecimalValues[i]) ? 1 : 0;
            }
            break;
          case STRING:
            String[] stringValues = _leftTransformFunction.transformToStringValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(stringValues[i]) ? 1 : 0;
            }
            break;
          case BYTES:
            byte[][] bytesValues = _leftTransformFunction.transformToBytesValuesSV(valueBlock);
            for (int i = 0; i < length; i++) {
              _intValuesSV[i] = _predicateEvaluator.applySV(bytesValues[i]) ? 1 : 0;
            }
            break;
          case UNKNOWN:
            fillResultUnknown(length);
            break;
          default:
            throw illegalState();
        }
      }
    } else {
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
        default:
          throw illegalState();
      }
    }
    return _intValuesSV;
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
