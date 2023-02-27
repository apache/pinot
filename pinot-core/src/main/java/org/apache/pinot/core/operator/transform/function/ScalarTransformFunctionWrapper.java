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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
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
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
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
        switch (literalTransformFunction.getResultMetadata().getDataType()) {
          case UNKNOWN:
            _scalarArguments[i] = null;
            break;
          case BOOLEAN:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getBooleanLiteral(), PinotDataType.BOOLEAN);
            break;
          case DOUBLE:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getDoubleLiteral(), PinotDataType.DOUBLE);
            break;
          case BIG_DECIMAL:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getBigDecimalLiteral(), PinotDataType.BIG_DECIMAL);
            break;
          default:
            _scalarArguments[i] =
                parameterTypes[i].convert(literalTransformFunction.getStringLiteral(), PinotDataType.STRING);
            break;
        }
      } else {
        _nonLiteralIndices[_numNonLiteralArguments] = i;
        _nonLiteralFunctions[_numNonLiteralArguments] = transformFunction;
        _numNonLiteralArguments++;
      }
    }
    _nonLiteralValues = new Object[_numNonLiteralArguments][];
    super.init(arguments, dataSourceMap);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[length];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments, true);
      if (result != null) {
        _intValuesSV[i] = (int) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      }
    }
    return _intValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, int[]> transformToIntValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    if (_intValuesSV == null) {
      _intValuesSV = new int[length];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments, false);
      if (result != null) {
        _intValuesSV[i] = (int) _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _intValuesSV);
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[length];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _longValuesSV[i] = (long) result;
      }
    }
    return _longValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, long[]> transformToLongValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[length];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _longValuesSV[i] = (long) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _longValuesSV);
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[length];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _floatValuesSV[i] = (float) result;
      }
    }
    return _floatValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, float[]> transformToFloatValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[length];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _floatValuesSV[i] = (float) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _floatValuesSV);
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[length];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _doubleValuesSV[i] = (double) result;
      }
    }
    return _doubleValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, double[]> transformToDoubleValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[length];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result == null) {
        bitmap.add(i);
      } else {
        _doubleValuesSV[i] = (double) result;
      }
    }
    return ImmutablePair.of(bitmap, _doubleValuesSV);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_bigDecimalValuesSV == null) {
      _bigDecimalValuesSV = new BigDecimal[length];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _bigDecimalValuesSV[i] = (BigDecimal) result;
      }
    }
    return _bigDecimalValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, BigDecimal[]> transformToBigDecimalValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_bigDecimalValuesSV == null) {
      _bigDecimalValuesSV = new BigDecimal[length];
    }
    RoaringBitmap bitmap = new RoaringBitmap();
    getNonLiteralValuesWithNull(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _bigDecimalValuesSV[i] = (BigDecimal) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _bigDecimalValuesSV);
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[length];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments, true);
      if (result != null) {
        _stringValuesSV[i] =
            _resultType == PinotDataType.STRING ? result.toString() : (String) _resultType.toInternal(result);
      }
    }
    return _stringValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, String[]> transformToStringValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[length];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _functionInvoker.invoke(_scalarArguments, false);
      if (result != null) {
        _stringValuesSV[i] =
            _resultType == PinotDataType.STRING ? result.toString() : (String) _resultType.toInternal(result);
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _stringValuesSV);
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_bytesValuesSV == null) {
      _bytesValuesSV = new byte[length][];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _bytesValuesSV[i] = (byte[]) result;
      }
    }
    return _bytesValuesSV;
  }

  @Override
  public Pair<RoaringBitmap, byte[][]> transformToBytesValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.BYTES) {
      return super.transformToBytesValuesSVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_bytesValuesSV == null) {
      _bytesValuesSV = new byte[length][];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _bytesValuesSV[i] = (byte[]) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _bytesValuesSV);
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_intValuesMV == null) {
      _intValuesMV = new int[length][];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _intValuesMV[i] = (int[]) result;
      }
    }
    return _intValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, int[][]> transformToIntValuesMVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.INT) {
      return super.transformToIntValuesMVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_intValuesMV == null) {
      _intValuesMV = new int[length][];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _intValuesMV[i] = (int[]) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _intValuesMV);
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_longValuesMV == null) {
      _longValuesMV = new long[length][];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _longValuesMV[i] = (long[]) result;
      }
    }
    return _longValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, long[][]> transformToLongValuesMVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.LONG) {
      return super.transformToLongValuesMVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_longValuesMV == null) {
      _longValuesMV = new long[length][];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _longValuesMV[i] = (long[]) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _longValuesMV);
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[length][];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _floatValuesMV[i] = (float[]) result;
      }
    }
    return _floatValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, float[][]> transformToFloatValuesMVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.FLOAT) {
      return super.transformToFloatValuesMVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_floatValuesMV == null) {
      _floatValuesMV = new float[length][];
    }
    RoaringBitmap bitmap = new RoaringBitmap();
    getNonLiteralValuesWithNull(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _floatValuesMV[i] = (float[]) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _floatValuesMV);
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[length][];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _doubleValuesMV[i] = (double[]) result;
      }
    }
    return _doubleValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, double[][]> transformToDoubleValuesMVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesMVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesMV == null) {
      _doubleValuesMV = new double[length][];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _doubleValuesMV[i] = (double[]) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _doubleValuesMV);
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMV(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[length][];
    }
    getNonLiteralValues(projectionBlock);
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, true));
      if (result != null) {
        _stringValuesMV[i] = (String[]) result;
      }
    }
    return _stringValuesMV;
  }

  @Override
  public Pair<RoaringBitmap, String[][]> transformToStringValuesMVWithNull(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() != DataType.STRING) {
      return super.transformToStringValuesMVWithNull(projectionBlock);
    }
    int length = projectionBlock.getNumDocs();
    if (_stringValuesMV == null) {
      _stringValuesMV = new String[length][];
    }
    getNonLiteralValuesWithNull(projectionBlock);
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < _numNonLiteralArguments; j++) {
        _scalarArguments[_nonLiteralIndices[j]] = _nonLiteralValues[j][i];
      }
      Object result = _resultType.toInternal(_functionInvoker.invoke(_scalarArguments, false));
      if (result != null) {
        _stringValuesMV[i] = (String[]) result;
      } else {
        bitmap.add(i);
      }
    }
    return ImmutablePair.of(bitmap, _stringValuesMV);
  }

  /**
   * Helper method to fetch values for the non-literal transform functions based on the parameter types.
   */
  private void getNonLiteralValues(ProjectionBlock projectionBlock) {
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    for (int i = 0; i < _numNonLiteralArguments; i++) {
      PinotDataType parameterType = parameterTypes[_nonLiteralIndices[i]];
      TransformFunction transformFunction = _nonLiteralFunctions[i];
      switch (parameterType) {
        case INTEGER:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToIntValuesSV(projectionBlock));
          break;
        case LONG:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToLongValuesSV(projectionBlock));
          break;
        case FLOAT:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToFloatValuesSV(projectionBlock));
          break;
        case DOUBLE:
          _nonLiteralValues[i] = ArrayUtils.toObject(transformFunction.transformToDoubleValuesSV(projectionBlock));
          break;
        case BIG_DECIMAL:
          _nonLiteralValues[i] = transformFunction.transformToBigDecimalValuesSV(projectionBlock);
          break;
        case BOOLEAN: {
          int[] intValues = transformFunction.transformToIntValuesSV(projectionBlock);
          int numValues = intValues.length;
          Boolean[] booleanValues = new Boolean[numValues];
          for (int j = 0; j < numValues; j++) {
            booleanValues[j] = intValues[j] == 1;
          }
          _nonLiteralValues[i] = booleanValues;
          break;
        }
        case TIMESTAMP: {
          long[] longValues = transformFunction.transformToLongValuesSV(projectionBlock);
          int numValues = longValues.length;
          Timestamp[] timestampValues = new Timestamp[numValues];
          for (int j = 0; j < numValues; j++) {
            timestampValues[j] = new Timestamp(longValues[j]);
          }
          _nonLiteralValues[i] = timestampValues;
          break;
        }
        case STRING:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesSV(projectionBlock);
          break;
        case BYTES:
          _nonLiteralValues[i] = transformFunction.transformToBytesValuesSV(projectionBlock);
          break;
        case PRIMITIVE_INT_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToIntValuesMV(projectionBlock);
          break;
        case PRIMITIVE_LONG_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToLongValuesMV(projectionBlock);
          break;
        case PRIMITIVE_FLOAT_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToFloatValuesMV(projectionBlock);
          break;
        case PRIMITIVE_DOUBLE_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToDoubleValuesMV(projectionBlock);
          break;
        case STRING_ARRAY:
          _nonLiteralValues[i] = transformFunction.transformToStringValuesMV(projectionBlock);
          break;
        default:
          throw new IllegalStateException("Unsupported parameter type: " + parameterType);
      }
    }
  }

  /**
   * Helper method to fetch values for the non-literal transform functions based on the parameter types.
   */
  private void getNonLiteralValuesWithNull(ProjectionBlock projectionBlock) {
    PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
    for (int i = 0; i < _numNonLiteralArguments; i++) {
      PinotDataType parameterType = parameterTypes[_nonLiteralIndices[i]];
      TransformFunction transformFunction = _nonLiteralFunctions[i];
      RoaringBitmap bitmap = null;
      switch (parameterType) {
        case INTEGER:
          Pair<RoaringBitmap, int[]> intResult = transformFunction.transformToIntValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(intResult.getRight());
          bitmap = intResult.getLeft();
          break;
        case LONG:
          Pair<RoaringBitmap, long[]> longResult = transformFunction.transformToLongValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(longResult.getRight());
          bitmap = longResult.getLeft();
          break;
        case FLOAT:
          Pair<RoaringBitmap, float[]> floatResult =
              transformFunction.transformToFloatValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(floatResult.getRight());
          bitmap = floatResult.getLeft();
          break;
        case DOUBLE:
          Pair<RoaringBitmap, double[]> doubleResult =
              transformFunction.transformToDoubleValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = ArrayUtils.toObject(doubleResult.getRight());
          bitmap = doubleResult.getLeft();
          break;
        case BIG_DECIMAL:
          Pair<RoaringBitmap, BigDecimal[]> bigdecimalResult =
              transformFunction.transformToBigDecimalValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = bigdecimalResult.getRight();
          bitmap = bigdecimalResult.getLeft();
          break;
        case BOOLEAN: {
          Pair<RoaringBitmap, int[]> boolResult = transformFunction.transformToIntValuesSVWithNull(projectionBlock);
          int numValues = boolResult.getRight().length;
          Boolean[] booleanValues = new Boolean[numValues];
          for (int j = 0; j < numValues; j++) {
            booleanValues[j] = boolResult.getValue()[j] == 1;
          }
          _nonLiteralValues[i] = booleanValues;
          bitmap = boolResult.getLeft();
          break;
        }
        case TIMESTAMP: {
          Pair<RoaringBitmap, long[]> timeResult = transformFunction.transformToLongValuesSVWithNull(projectionBlock);
          int numValues = timeResult.getRight().length;
          Timestamp[] timestampValues = new Timestamp[numValues];
          for (int j = 0; j < numValues; j++) {
            timestampValues[j] = new Timestamp(timeResult.getRight()[j]);
          }
          _nonLiteralValues[i] = timestampValues;
          bitmap = timeResult.getLeft();
          break;
        }
        case STRING:
          Pair<RoaringBitmap, String[]> stringResult =
              transformFunction.transformToStringValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = stringResult.getRight();
          bitmap = stringResult.getLeft();
          break;
        case BYTES:
          Pair<RoaringBitmap, byte[][]> byteResult =
              transformFunction.transformToBytesValuesSVWithNull(projectionBlock);
          _nonLiteralValues[i] = byteResult.getRight();
          bitmap = byteResult.getLeft();
          break;
        case PRIMITIVE_INT_ARRAY:
          Pair<RoaringBitmap, int[][]> intMVResult = transformFunction.transformToIntValuesMVWithNull(projectionBlock);
          _nonLiteralValues[i] = intMVResult.getRight();
          bitmap = intMVResult.getLeft();
          break;
        case PRIMITIVE_LONG_ARRAY:
          Pair<RoaringBitmap, long[][]> longMVResult =
              transformFunction.transformToLongValuesMVWithNull(projectionBlock);
          _nonLiteralValues[i] = longMVResult.getRight();
          bitmap = longMVResult.getLeft();
          break;
        case PRIMITIVE_FLOAT_ARRAY:
          Pair<RoaringBitmap, float[][]> floatMVResult =
              transformFunction.transformToFloatValuesMVWithNull(projectionBlock);
          _nonLiteralValues[i] = floatMVResult.getRight();
          bitmap = floatMVResult.getLeft();
          break;
        case PRIMITIVE_DOUBLE_ARRAY:
          Pair<RoaringBitmap, double[][]> doubleMVResult =
              transformFunction.transformToDoubleValuesMVWithNull(projectionBlock);
          _nonLiteralValues[i] = doubleMVResult.getRight();
          bitmap = doubleMVResult.getLeft();
          break;
        case STRING_ARRAY:
          Pair<RoaringBitmap, String[][]> stringMVResult =
              transformFunction.transformToStringValuesMVWithNull(projectionBlock);
          _nonLiteralValues[i] = stringMVResult.getRight();
          bitmap = stringMVResult.getLeft();
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
