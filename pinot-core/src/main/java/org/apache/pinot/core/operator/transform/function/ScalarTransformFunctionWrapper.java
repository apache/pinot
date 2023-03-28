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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Wrapper transform function on the annotated scalar function.
 */
public class ScalarTransformFunctionWrapper extends BaseTransformFunction {
  private final String _name;
  private final FunctionInvoker _functionInvoker;
  private final PinotDataType _resultType;
  private final TransformResultMetadata _resultMetadata;

  private Object[] _arguments;
  private int _numNonLiteralArguments;
  private int[] _nonLiteralIndices;
  private TransformFunction[] _nonLiteralFunctions;
  private Object[][] _nonLiteralValues;

  public ScalarTransformFunctionWrapper(FunctionInfo functionInfo) {
    _name = functionInfo.getMethod().getName();
    _functionInvoker = new FunctionInvoker(functionInfo);
    Class<?>[] parameterClasses = _functionInvoker.getParameterClasses();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    int numParameters = parameterClasses.length;
    for (int i = 0; i < numParameters; i++) {
      Preconditions.checkArgument(parameterTypes[i] != null, "Unsupported parameter class: %s for method: %s",
          parameterClasses[i], functionInfo.getMethod());
    }
    Class<?> resultClass = _functionInvoker.getResultClass();
    PinotDataType resultType = FunctionUtils.getParameterType(resultClass);
    if (resultType != null) {
      _resultType = resultType;
      _resultMetadata =
          new TransformResultMetadata(FunctionUtils.getDataType(resultClass), _resultType.isSingleValue(), false);
    } else {
      // Handle unrecognized result class with STRING
      _resultType = PinotDataType.STRING;
      _resultMetadata = new TransformResultMetadata(DataType.STRING, true, false);
    }
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    int numArguments = arguments.size();
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    Preconditions.checkArgument(numArguments == parameterTypes.length,
        "Wrong number of arguments for method: %s, expected: %s, actual: %s", _functionInvoker.getMethod(),
        parameterTypes.length, numArguments);

    _arguments = new Object[numArguments];
    _nonLiteralIndices = new int[numArguments];
    _nonLiteralFunctions = new TransformFunction[numArguments];
    for (int i = 0; i < numArguments; i++) {
      TransformFunction transformFunction = arguments.get(i);
      if (transformFunction instanceof LiteralTransformFunction) {
        String literal = ((LiteralTransformFunction) transformFunction).getStringLiteral();
        _arguments[i] = parameterTypes[i].convert(literal, PinotDataType.STRING);
      } else {
        _nonLiteralIndices[_numNonLiteralArguments] = i;
        _nonLiteralFunctions[_numNonLiteralArguments] = transformFunction;
        _numNonLiteralArguments++;
      }
    }
    _nonLiteralValues = new Object[_numNonLiteralArguments][];
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
    int length = valueBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[length];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _intValuesSV[i] = (int) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[length];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _longValuesSV[i] = (long) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _longValuesSV;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[length];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _floatValuesSV[i] = (float) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _floatValuesSV;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[length];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _doubleValuesSV[i] = (double) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _doubleValuesSV;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_bigDecimalValuesSV == null) {
      _bigDecimalValuesSV = new BigDecimal[length];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _bigDecimalValuesSV[i] = (BigDecimal) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[length];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_arguments);
      _stringValuesSV[i] =
          _resultType == PinotDataType.STRING ? result.toString() : (String) _resultType.toInternal(result);
    }
    return _stringValuesSV;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_bytesValuesSV == null) {
      _bytesValuesSV = new byte[length][];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _bytesValuesSV[i] = (byte[]) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _bytesValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_intValuesMV == null) {
      _intValuesMV = new int[length][];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _intValuesMV[i] = (int[]) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_longValuesMV == null) {
      _longValuesMV = new long[length][];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _longValuesMV[i] = (long[]) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _longValuesMV;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[length][];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _floatValuesMV[i] = (float[]) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _floatValuesMV;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[length][];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _doubleValuesMV[i] = (double[]) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _doubleValuesMV;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[length][];
    }
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _arguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _stringValuesMV[i] = (String[]) _resultType.toInternal(_functionInvoker.invoke(_arguments));
    }
    return _stringValuesMV;
  }

  /**
   * Helper method to fetch values for the non-literal transform functions based on the parameter types.
   */
  private void getNonLiteralValues(ValueBlock valueBlock) {
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    for (int i = 0; i < _numNonLiteralArguments; i++) {
      PinotDataType parameterType = parameterTypes[_nonLiteralIndices[i]];
      TransformFunction transformFunction = _nonLiteralFunctions[i];
      switch (parameterType) {
        case INTEGER:
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
        case STRING_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesMV(valueBlock);
          break;
        default:
          throw new IllegalStateException("Unsupported parameter type: " + parameterType);
      }
    }
  }
}
