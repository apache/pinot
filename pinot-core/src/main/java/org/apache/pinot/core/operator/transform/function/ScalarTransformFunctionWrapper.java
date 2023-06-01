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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


/**
 * Wrapper transform function on the annotated scalar function.
 */
public class ScalarTransformFunctionWrapper extends BaseTransformFunction {
  private final String _name;
  private final FunctionInvoker _functionInvoker;
  private final PinotDataType _resultType;
  private final TransformResultMetadata _resultMetadata;

  private Object[] _scalarArguments;
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

    _scalarArguments = new Object[numArguments];
    _nonLiteralIndices = new int[numArguments];
    _nonLiteralFunctions = new TransformFunction[numArguments];
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
                parameterTypes[i].convert(literalTransformFunction.getIntLiteral(), PinotDataType.INTEGER);
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
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getBigDecimalLiteral(), PinotDataType.BIG_DECIMAL);
            break;
          case TIMESTAMP:
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
    initIntValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _intValuesSV[i] = (int) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _intValuesSV;
  }

  @Override
  public Pair<int[], RoaringBitmap> transformToIntValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initIntValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _intValuesSV[i] = (int) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_intValuesSV, bitmap);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _longValuesSV[i] = (long) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _longValuesSV;
  }

  @Override
  public Pair<long[], RoaringBitmap> transformToLongValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initLongValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _longValuesSV[i] = (long) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_longValuesSV, bitmap);
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _floatValuesSV[i] = (float) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _floatValuesSV;
  }

  @Override
  public Pair<float[], RoaringBitmap> transformToFloatValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initFloatValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _floatValuesSV[i] = (float) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_floatValuesSV, bitmap);
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _doubleValuesSV[i] = (double) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _doubleValuesSV;
  }

  @Override
  public Pair<double[], RoaringBitmap> transformToDoubleValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initDoubleValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _doubleValuesSV[i] = (double) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_doubleValuesSV, bitmap);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _bigDecimalValuesSV[i] = (BigDecimal) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public Pair<BigDecimal[], RoaringBitmap> transformToBigDecimalValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initBigDecimalValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _bigDecimalValuesSV[i] = (BigDecimal) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_bigDecimalValuesSV, bitmap);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      _stringValuesSV[i] =
          _resultType == PinotDataType.STRING ? result.toString() : (String) _resultType.toInternal(result);
    }
    return _stringValuesSV;
  }

  @Override
  public Pair<String[], RoaringBitmap> transformToStringValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initStringValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _stringValuesSV[i] = (String) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_stringValuesSV, bitmap);
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initBytesValuesSV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _bytesValuesSV[i] = (byte[]) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _bytesValuesSV;
  }

  @Override
  public Pair<byte[][], RoaringBitmap> transformToBytesValuesSVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initBytesValuesSV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _bytesValuesSV[i] = (byte[]) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_bytesValuesSV, bitmap);
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _intValuesMV[i] = (int[]) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _intValuesMV;
  }

  @Override
  public Pair<int[][], RoaringBitmap> transformToIntValuesMVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initIntValuesMV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _intValuesMV[i] = (int[]) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_intValuesMV, bitmap);
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _longValuesMV[i] = (long[]) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _longValuesMV;
  }

  @Override
  public Pair<long[][], RoaringBitmap> transformToLongValuesMVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initLongValuesMV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _longValuesMV[i] = (long[]) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_longValuesMV, bitmap);
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _floatValuesMV[i] = (float[]) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _floatValuesMV;
  }

  @Override
  public Pair<float[][], RoaringBitmap> transformToFloatValuesMVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initFloatValuesMV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _floatValuesMV[i] = (float[]) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_floatValuesMV, bitmap);
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _doubleValuesMV[i] = (double[]) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _doubleValuesMV;
  }

  @Override
  public Pair<double[][], RoaringBitmap> transformToDoubleValuesMVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initDoubleValuesMV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _doubleValuesMV[i] = (double[]) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_doubleValuesMV, bitmap);
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    getNonLiteralValues(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      _stringValuesMV[i] = (String[]) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments));
    }
    return _stringValuesMV;
  }

  @Override
  public Pair<String[][], RoaringBitmap> transformToStringValuesMVWithNull(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMVWithNull(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    initStringValuesMV(length);
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result != null) {
        _stringValuesMV[i] = (String[]) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(_stringValuesMV, bitmap);
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    getNonLiteralValuesWithNull(valueBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments);
      if (result == null) {
        bitmap.add(i);
      }
    }
    if (bitmap != null && bitmap.isEmpty()) {
      return null;
    }
    return bitmap;
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

  /**
   * Helper method to fetch values with null for the non-literal transform functions based on the parameter types.
   */
  private void getNonLiteralValuesWithNull(ValueBlock valueBlock) {
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    for (int i = 0; i < _numNonLiteralArguments; i++) {
      PinotDataType parameterType = parameterTypes[_nonLiteralIndices[i]];
      TransformFunction transformFunction = _nonLiteralFunctions[i];
      RoaringBitmap bitmap = null;
      switch (parameterType) {
        case INTEGER:
          Pair<int[], RoaringBitmap> intResult = transformFunction.transformToIntValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(intResult.getLeft());
          bitmap = intResult.getRight();
          break;
        case LONG:
          Pair<long[], RoaringBitmap> longResult = transformFunction.transformToLongValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(longResult.getLeft());
          bitmap = longResult.getRight();
          break;
        case FLOAT:
          Pair<float[], RoaringBitmap> floatResult = transformFunction.transformToFloatValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(floatResult.getLeft());
          bitmap = floatResult.getRight();
          break;
        case DOUBLE:
          Pair<double[], RoaringBitmap> doubleResult = transformFunction.transformToDoubleValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(doubleResult.getLeft());
          bitmap = doubleResult.getRight();
          break;
        case BIG_DECIMAL:
          Pair<BigDecimal[], RoaringBitmap> bigDecimalResult =
              transformFunction.transformToBigDecimalValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = bigDecimalResult.getLeft();
          bitmap = bigDecimalResult.getRight();
          break;
        case BOOLEAN: {
          Pair<int[], RoaringBitmap> boolResult = transformFunction.transformToIntValuesSVWithNull(valueBlock);
          int numValues = boolResult.getLeft().length;
          Boolean[] booleanValues = new Boolean[numValues];
          for (int j = 0; j < numValues; j++) {
            booleanValues[j] = boolResult.getLeft()[j] == 1;
          }
          _nonLiteralValues[i] = booleanValues;
          bitmap = boolResult.getRight();
          break;
        }
        case TIMESTAMP: {
          Pair<long[], RoaringBitmap> timeResult = transformFunction.transformToLongValuesSVWithNull(valueBlock);
          int numValues = timeResult.getLeft().length;
          Timestamp[] timestampValues = new Timestamp[numValues];
          for (int j = 0; j < numValues; j++) {
            timestampValues[j] = new Timestamp(timeResult.getLeft()[j]);
          }
          _nonLiteralValues[i] = timestampValues;
          bitmap = timeResult.getRight();
          break;
        }
        case STRING:
          Pair<String[], RoaringBitmap> stringResult = transformFunction.transformToStringValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = stringResult.getLeft();
          bitmap = stringResult.getRight();
          break;
        case BYTES:
          Pair<byte[][], RoaringBitmap> byteResult = transformFunction.transformToBytesValuesSVWithNull(valueBlock);
          _nonLiteralValues[i] = byteResult.getLeft();
          bitmap = byteResult.getRight();
          break;
        case PRIMITIVE_INT_ARRAY:
          Pair<int[][], RoaringBitmap> intMVResult = transformFunction.transformToIntValuesMVWithNull(valueBlock);
          _nonLiteralValues[i] = intMVResult.getLeft();
          bitmap = intMVResult.getRight();
          break;
        case PRIMITIVE_LONG_ARRAY:
          Pair<long[][], RoaringBitmap> longMVResult = transformFunction.transformToLongValuesMVWithNull(valueBlock);
          _nonLiteralValues[i] = longMVResult.getLeft();
          bitmap = longMVResult.getRight();
          break;
        case PRIMITIVE_FLOAT_ARRAY:
          Pair<float[][], RoaringBitmap> floatMVResult = transformFunction.transformToFloatValuesMVWithNull(valueBlock);
          _nonLiteralValues[i] = floatMVResult.getLeft();
          bitmap = floatMVResult.getRight();
          break;
        case PRIMITIVE_DOUBLE_ARRAY:
          Pair<double[][], RoaringBitmap> doubleMVResult =
              transformFunction.transformToDoubleValuesMVWithNull(valueBlock);
          _nonLiteralValues[i] = doubleMVResult.getLeft();
          bitmap = doubleMVResult.getRight();
          break;
        case STRING_ARRAY:
          Pair<String[][], RoaringBitmap> stringMVResult =
              transformFunction.transformToStringValuesMVWithNull(valueBlock);
          _nonLiteralValues[i] = stringMVResult.getLeft();
          bitmap = stringMVResult.getRight();
          break;
        default:
          throw new IllegalStateException("Unsupported parameter type: " + parameterType);
      }
      if (bitmap != null) {
        int finalI = i;
        bitmap.forEach((IntConsumer) (j) -> {
          _nonLiteralValues[finalI][j] = null;
        });
      }
    }
  }
}
