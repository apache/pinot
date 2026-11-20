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
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.function.QueryFunctionInvoker;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.apache.pinot.spi.utils.PinotDataType;
import org.apache.pinot.spi.utils.UuidUtils;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/// Wrapper transform function on the annotated scalar function.
///
/// When null handling is enabled and the function accepts `null` arguments (see `ScalarFunction#nullableParameters`),
/// the function is invoked with `null` for the null rows of the arguments, and the rows where it returns `null` are the
/// null rows of the result. Otherwise, the function is invoked with the values stored in the null rows (e.g. the
/// default null value of a column), and a row of the result is null when any argument is null in it.
public class ScalarTransformFunctionWrapper extends BaseTransformFunction {
  private final String _name;
  private final QueryFunctionInvoker _functionInvoker;
  private final boolean _nullableParameters;
  private final ColumnDataType _resultType;
  private final TransformResultMetadata _resultMetadata;

  private Object[] _scalarArguments;
  private int _numNonLiteralArguments;
  private int[] _nonLiteralIndices;
  private TransformFunction[] _nonLiteralFunctions;
  private PinotDataType[] _nonLiteralTypes;
  private Object[][] _nonLiteralValues;

  // Whether the function is invoked with null for null argument rows, and decides the null rows of the result
  private boolean _invokeWithNulls;
  // When invoking with nulls, the value block evaluated last, whose values are in the result buffer, and the rows of it
  // where the function returns null. Value blocks are not reused across batches, so these stay valid for the block.
  @Nullable
  private ValueBlock _evaluatedValueBlock;
  @Nullable
  private RoaringBitmap _resultNullBitmap;

  public ScalarTransformFunctionWrapper(FunctionInfo functionInfo) {
    _name = functionInfo.getMethod().getName();
    _functionInvoker = new QueryFunctionInvoker(functionInfo);
    _nullableParameters = functionInfo.hasNullableParameters();
    Class<?>[] parameterClasses = _functionInvoker.getParameterClasses();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    int numParameters = parameterClasses.length;
    for (int i = 0; i < numParameters; i++) {
      Preconditions.checkArgument(parameterTypes[i] != null, "Unsupported parameter class: %s for method: %s",
          parameterClasses[i], functionInfo.getMethod());
    }
    Class<?> resultClass = _functionInvoker.getResultClass();
    ColumnDataType resultType = FunctionUtils.getColumnDataType(resultClass);
    if (resultType != null) {
      _resultType = resultType;
      _resultMetadata = new TransformResultMetadata(resultType.toDataType(), !_resultType.isArray(), false);
    } else {
      // Handle unrecognized result class with STRING
      _resultType = ColumnDataType.STRING;
      _resultMetadata = new TransformResultMetadata(DataType.STRING, true, false);
    }
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArguments = arguments.size();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    Preconditions.checkArgument(numArguments == parameterTypes.length,
        "Wrong number of arguments for method: %s, expected: %s, actual: %s", _functionInvoker.getMethod(),
        parameterTypes.length, numArguments);

    _scalarArguments = new Object[numArguments];
    _nonLiteralIndices = new int[numArguments];
    _nonLiteralFunctions = new TransformFunction[numArguments];
    _nonLiteralTypes = new PinotDataType[numArguments];
    for (int i = 0; i < numArguments; i++) {
      TransformFunction transformFunction = arguments.get(i);
      if (transformFunction instanceof LiteralTransformFunction) {
        LiteralTransformFunction literalTransformFunction = (LiteralTransformFunction) transformFunction;
        DataType dataType = literalTransformFunction.getResultMetadata().getDataType();
        switch (dataType) {
          case BOOLEAN:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getBooleanLiteral(), PinotDataType.BOOLEAN);
            break;
          case INT:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getIntLiteral(), PinotDataType.INT);
            break;
          case LONG:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getLongLiteral(), PinotDataType.LONG);
            break;
          case FLOAT:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getFloatLiteral(), PinotDataType.FLOAT);
            break;
          case DOUBLE:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getDoubleLiteral(), PinotDataType.DOUBLE);
            break;
          case BIG_DECIMAL:
            if (parameterTypes[i] == PinotDataType.STRING) {
              _scalarArguments[i] = literalTransformFunction.getStringLiteral();
              break;
            }
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getBigDecimalLiteral(), PinotDataType.BIG_DECIMAL);
            break;
          case TIMESTAMP:
            if (parameterTypes[i] == PinotDataType.STRING) {
              _scalarArguments[i] = literalTransformFunction.getStringLiteral();
              break;
            }
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getLongLiteral(), PinotDataType.TIMESTAMP);
            break;
          case STRING:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getStringLiteral(), PinotDataType.STRING);
            break;
          case UNKNOWN:
            _scalarArguments[i] = null;
            break;
          default:
            throw new RuntimeException("Unsupported data type:" + dataType);
        }
      } else {
        _nonLiteralIndices[_numNonLiteralArguments] = i;
        _nonLiteralFunctions[_numNonLiteralArguments] = transformFunction;
        _nonLiteralTypes[_numNonLiteralArguments] = parameterTypes[i] != PinotDataType.OBJECT
            ? parameterTypes[i]
            : getObjectArgumentType(transformFunction.getResultMetadata());
        _numNonLiteralArguments++;
      }
    }
    _nonLiteralValues = new Object[_numNonLiteralArguments][];
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    _invokeWithNulls = nullHandlingEnabled && _nullableParameters;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _intValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _intValuesSV[i] = (int) _resultType.toInternal(value);
      } else {
        _intValuesSV[i] = NullValuePlaceHolder.INT;
        addNullResult(i);
      }
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _longValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _longValuesSV[i] = (long) _resultType.toInternal(value);
      } else {
        _longValuesSV[i] = NullValuePlaceHolder.LONG;
        addNullResult(i);
      }
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _floatValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _floatValuesSV[i] = (float) _resultType.toInternal(value);
      } else {
        _floatValuesSV[i] = NullValuePlaceHolder.FLOAT;
        addNullResult(i);
      }
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _doubleValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _doubleValuesSV[i] = (double) _resultType.toInternal(value);
      } else {
        _doubleValuesSV[i] = NullValuePlaceHolder.DOUBLE;
        addNullResult(i);
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _bigDecimalValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _bigDecimalValuesSV[i] = (BigDecimal) _resultType.toInternal(value);
      } else {
        _bigDecimalValuesSV[i] = NullValuePlaceHolder.BIG_DECIMAL;
        addNullResult(i);
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _stringValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _stringValuesSV[i] = (String) _resultType.toInternal(value);
      } else {
        _stringValuesSV[i] = NullValuePlaceHolder.STRING;
        addNullResult(i);
      }
    }
    return _stringValuesSV;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _bytesValuesSV;
    }
    int length = valueBlock.getNumDocs();
    initBytesValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _bytesValuesSV[i] = ((ByteArray) _resultType.toInternal(value)).getBytes();
      } else {
        _bytesValuesSV[i] = NullValuePlaceHolder.BYTES;
        addNullResult(i);
      }
    }
    return _bytesValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _intValuesMV;
    }
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _intValuesMV[i] = (int[]) _resultType.toInternal(value);
      } else {
        _intValuesMV[i] = NullValuePlaceHolder.INT_ARRAY;
        addNullResult(i);
      }
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _longValuesMV;
    }
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _longValuesMV[i] = (long[]) _resultType.toInternal(value);
      } else {
        _longValuesMV[i] = NullValuePlaceHolder.LONG_ARRAY;
        addNullResult(i);
      }
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _floatValuesMV;
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _floatValuesMV[i] = (float[]) _resultType.toInternal(value);
      } else {
        _floatValuesMV[i] = NullValuePlaceHolder.FLOAT_ARRAY;
        addNullResult(i);
      }
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _doubleValuesMV;
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _doubleValuesMV[i] = (double[]) _resultType.toInternal(value);
      } else {
        _doubleValuesMV[i] = NullValuePlaceHolder.DOUBLE_ARRAY;
        addNullResult(i);
      }
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    if (valueBlock == _evaluatedValueBlock) {
      return _stringValuesMV;
    }
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object value = _functionInvoker.invoke(_scalarArguments);
      if (value != null) {
        _stringValuesMV[i] = (String[]) _resultType.toInternal(value);
      } else {
        _stringValuesMV[i] = NullValuePlaceHolder.STRING_ARRAY;
        addNullResult(i);
      }
    }
    return _stringValuesMV;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    if (!_invokeWithNulls) {
      return super.getNullBitmap(valueBlock);
    }
    if (valueBlock != _evaluatedValueBlock) {
      evaluate(valueBlock);
    }
    return _resultNullBitmap;
  }

  /// Evaluates the function on the given value block with the transform method of the result type, which also records
  /// the null rows of the result.
  private void evaluate(ValueBlock valueBlock) {
    DataType storedType = _resultMetadata.getDataType().getStoredType();
    if (_resultMetadata.isSingleValue()) {
      switch (storedType) {
        case INT:
          transformToIntValuesSV(valueBlock);
          break;
        case LONG:
          transformToLongValuesSV(valueBlock);
          break;
        case FLOAT:
          transformToFloatValuesSV(valueBlock);
          break;
        case DOUBLE:
          transformToDoubleValuesSV(valueBlock);
          break;
        case BIG_DECIMAL:
          transformToBigDecimalValuesSV(valueBlock);
          break;
        case STRING:
          transformToStringValuesSV(valueBlock);
          break;
        case BYTES:
          transformToBytesValuesSV(valueBlock);
          break;
        default:
          throw new IllegalStateException("Unsupported single-value result type: " + storedType);
      }
    } else {
      switch (storedType) {
        case INT:
          transformToIntValuesMV(valueBlock);
          break;
        case LONG:
          transformToLongValuesMV(valueBlock);
          break;
        case FLOAT:
          transformToFloatValuesMV(valueBlock);
          break;
        case DOUBLE:
          transformToDoubleValuesMV(valueBlock);
          break;
        case STRING:
          transformToStringValuesMV(valueBlock);
          break;
        default:
          throw new IllegalStateException("Unsupported multi-value result type: " + storedType);
      }
    }
  }

  private void addNullResult(int row) {
    if (_invokeWithNulls) {
      if (_resultNullBitmap == null) {
        _resultNullBitmap = new RoaringBitmap();
      }
      _resultNullBitmap.add(row);
    }
  }

  /// Returns the type to read an argument for an `Object` parameter, so that the function gets the external Java value
  /// of the argument result type, the same as `ColumnDataType.toExternal()` in the multi-stage engine `FunctionOperand`
  /// for that type. Complex types such as MAP are not supported.
  private PinotDataType getObjectArgumentType(TransformResultMetadata argumentMetadata) {
    DataType dataType = argumentMetadata.getDataType();
    boolean singleValue = argumentMetadata.isSingleValue();
    switch (dataType) {
      case INT:
        return singleValue ? PinotDataType.INT : PinotDataType.PRIMITIVE_INT_ARRAY;
      case LONG:
        return singleValue ? PinotDataType.LONG : PinotDataType.PRIMITIVE_LONG_ARRAY;
      case FLOAT:
        return singleValue ? PinotDataType.FLOAT : PinotDataType.PRIMITIVE_FLOAT_ARRAY;
      case DOUBLE:
        return singleValue ? PinotDataType.DOUBLE : PinotDataType.PRIMITIVE_DOUBLE_ARRAY;
      case BIG_DECIMAL:
        return singleValue ? PinotDataType.BIG_DECIMAL : PinotDataType.BIG_DECIMAL_ARRAY;
      case BOOLEAN:
        return singleValue ? PinotDataType.BOOLEAN : PinotDataType.PRIMITIVE_BOOLEAN_ARRAY;
      case TIMESTAMP:
        return singleValue ? PinotDataType.TIMESTAMP : PinotDataType.TIMESTAMP_ARRAY;
      case STRING:
      case JSON:
        return singleValue ? PinotDataType.STRING : PinotDataType.STRING_ARRAY;
      case BYTES:
        return singleValue ? PinotDataType.BYTES : PinotDataType.BYTES_ARRAY;
      case UUID:
        return singleValue ? PinotDataType.UUID : PinotDataType.UUID_ARRAY;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported argument type: %s for Object parameter of method: %s", dataType,
                _functionInvoker.getMethod()));
    }
  }

  /// Helper method to fetch values for the non-literal transform functions based on the parameter types. For an
  /// `Object` parameter, the values are read based on the argument result type (see [#getObjectArgumentType]).
  ///
  /// When invoking with nulls, also replaces the values of the null rows with `null`, and starts recording the null
  /// rows of the result for the value block.
  private void getNonLiteralValues(ValueBlock valueBlock) {
    for (int i = 0; i < _numNonLiteralArguments; i++) {
      PinotDataType parameterType = _nonLiteralTypes[i];
      TransformFunction transformFunction = _nonLiteralFunctions[i];
      switch (parameterType) {
        case INT:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToIntValuesSV(valueBlock));
          break;
        case LONG:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToLongValuesSV(valueBlock));
          break;
        case FLOAT:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToFloatValuesSV(valueBlock));
          break;
        case DOUBLE:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToDoubleValuesSV(valueBlock));
          break;
        case BIG_DECIMAL:
          _nonLiteralValues[i] = transformFunction.transformToBigDecimalValuesSV(valueBlock);
          break;
        case BOOLEAN: {
          int[] intValues = transformFunction.transformToIntValuesSV(valueBlock);
          int numValues = intValues.length;
          Boolean[] booleanValues = new Boolean[numValues];
          for (int j = 0; j < numValues; j++) {
            booleanValues[j] = intValues[j] == 1;
          }
          _nonLiteralValues[i] = booleanValues;
          break;
        }
        case TIMESTAMP: {
          long[] longValues = transformFunction.transformToLongValuesSV(valueBlock);
          int numValues = longValues.length;
          Timestamp[] timestampValues = new Timestamp[numValues];
          for (int j = 0; j < numValues; j++) {
            timestampValues[j] = new Timestamp(longValues[j]);
          }
          _nonLiteralValues[i] = timestampValues;
          break;
        }
        case STRING:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesSV(valueBlock);
          break;
        case BYTES:
          _nonLiteralValues[i] = transformFunction.transformToBytesValuesSV(valueBlock);
          break;
        case UUID: {
          byte[][] bytesValues = transformFunction.transformToBytesValuesSV(valueBlock);
          int numValues = bytesValues.length;
          UUID[] uuidValues = new UUID[numValues];
          for (int j = 0; j < numValues; j++) {
            uuidValues[j] = UuidUtils.toUUID(bytesValues[j]);
          }
          _nonLiteralValues[i] = uuidValues;
          break;
        }
        case PRIMITIVE_INT_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToIntValuesMV(valueBlock);
          break;
        case PRIMITIVE_LONG_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToLongValuesMV(valueBlock);
          break;
        case PRIMITIVE_FLOAT_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToFloatValuesMV(valueBlock);
          break;
        case PRIMITIVE_DOUBLE_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToDoubleValuesMV(valueBlock);
          break;
        case BIG_DECIMAL_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToBigDecimalValuesMV(valueBlock);
          break;
        case PRIMITIVE_BOOLEAN_ARRAY: {
          int[][] intValuesMV = transformFunction.transformToIntValuesMV(valueBlock);
          int numRows = intValuesMV.length;
          boolean[][] booleanValuesMV = new boolean[numRows][];
          for (int j = 0; j < numRows; j++) {
            int[] intValues = intValuesMV[j];
            int numValues = intValues.length;
            boolean[] booleanValues = new boolean[numValues];
            for (int k = 0; k < numValues; k++) {
              booleanValues[k] = intValues[k] == 1;
            }
            booleanValuesMV[j] = booleanValues;
          }
          _nonLiteralValues[i] = booleanValuesMV;
          break;
        }
        case TIMESTAMP_ARRAY: {
          long[][] longValuesMV = transformFunction.transformToLongValuesMV(valueBlock);
          int numRows = longValuesMV.length;
          Timestamp[][] timestampValuesMV = new Timestamp[numRows][];
          for (int j = 0; j < numRows; j++) {
            long[] longValues = longValuesMV[j];
            int numValues = longValues.length;
            Timestamp[] timestampValues = new Timestamp[numValues];
            for (int k = 0; k < numValues; k++) {
              timestampValues[k] = new Timestamp(longValues[k]);
            }
            timestampValuesMV[j] = timestampValues;
          }
          _nonLiteralValues[i] = timestampValuesMV;
          break;
        }
        case STRING_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesMV(valueBlock);
          break;
        case BYTES_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToBytesValuesMV(valueBlock);
          break;
        case UUID_ARRAY: {
          byte[][][] bytesValuesMV = transformFunction.transformToBytesValuesMV(valueBlock);
          int numRows = bytesValuesMV.length;
          UUID[][] uuidValuesMV = new UUID[numRows][];
          for (int j = 0; j < numRows; j++) {
            byte[][] bytesValues = bytesValuesMV[j];
            int numValues = bytesValues.length;
            UUID[] uuidValues = new UUID[numValues];
            for (int k = 0; k < numValues; k++) {
              uuidValues[k] = UuidUtils.toUUID(bytesValues[k]);
            }
            uuidValuesMV[j] = uuidValues;
          }
          _nonLiteralValues[i] = uuidValuesMV;
          break;
        }
        default:
          throw new IllegalStateException("Unsupported parameter type: " + parameterType);
      }
    }
    if (_invokeWithNulls) {
      for (int i = 0; i < _numNonLiteralArguments; i++) {
        RoaringBitmap nullBitmap = _nonLiteralFunctions[i].getNullBitmap(valueBlock);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          // The values can be the buffer of the argument, so replace the null rows in a copy
          Object[] values = _nonLiteralValues[i].clone();
          PeekableIntIterator iterator = nullBitmap.getIntIterator();
          while (iterator.hasNext()) {
            values[iterator.next()] = null;
          }
          _nonLiteralValues[i] = values;
        }
      }
      _evaluatedValueBlock = valueBlock;
      _resultNullBitmap = null;
    }
  }
}
