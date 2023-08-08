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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>LiteralTransformFunction</code> class is a special transform function which is a wrapper on top of a
 * LITERAL. The data type is inferred from the literal string.
 */
public class ArrayLiteralTransformFunction implements TransformFunction {
  public static final String FUNCTION_NAME = "arrayValueConstructor";

  private final DataType _dataType;

  private final int[] _intArrayLiteral;
  private final long[] _longArrayLiteral;
  private final float[] _floatArrayLiteral;
  private final double[] _doubleArrayLiteral;
  private final String[] _stringArrayLiteral;

  // literals may be shared but values are intentionally not volatile as assignment races are benign
  private int[][] _intArrayResult;
  private long[][] _longArrayResult;
  private float[][] _floatArrayResult;
  private double[][] _doubleArrayResult;
  private String[][] _stringArrayResult;

  public ArrayLiteralTransformFunction(List<ExpressionContext> literalContexts) {
    Preconditions.checkNotNull(literalContexts);
    if (literalContexts.isEmpty()) {
      _dataType = DataType.UNKNOWN;
      _intArrayLiteral = new int[0];
      _longArrayLiteral = new long[0];
      _floatArrayLiteral = new float[0];
      _doubleArrayLiteral = new double[0];
      _stringArrayLiteral = new String[0];
      return;
    }
    for (ExpressionContext literalContext : literalContexts) {
      Preconditions.checkState(literalContext.getType() == ExpressionContext.Type.LITERAL,
          "ArrayLiteralTransformFunction only takes literals as arguments, found: %s", literalContext);
    }
    _dataType = literalContexts.get(0).getLiteral().getType();
    switch (_dataType) {
      case INT:
        _intArrayLiteral = new int[literalContexts.size()];
        for (int i = 0; i < _intArrayLiteral.length; i++) {
          _intArrayLiteral[i] = literalContexts.get(i).getLiteral().getIntValue();
        }
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      case LONG:
        _longArrayLiteral = new long[literalContexts.size()];
        for (int i = 0; i < _longArrayLiteral.length; i++) {
          _longArrayLiteral[i] = Long.parseLong(literalContexts.get(i).getLiteral().getStringValue());
        }
        _intArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      case FLOAT:
        _floatArrayLiteral = new float[literalContexts.size()];
        for (int i = 0; i < _floatArrayLiteral.length; i++) {
          _floatArrayLiteral[i] = Float.parseFloat(literalContexts.get(i).getLiteral().getStringValue());
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _doubleArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      case DOUBLE:
        _doubleArrayLiteral = new double[literalContexts.size()];
        for (int i = 0; i < _doubleArrayLiteral.length; i++) {
          _doubleArrayLiteral[i] = Double.parseDouble(literalContexts.get(i).getLiteral().getStringValue());
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      case STRING:
        _stringArrayLiteral = new String[literalContexts.size()];
        for (int i = 0; i < _stringArrayLiteral.length; i++) {
          _stringArrayLiteral[i] = literalContexts.get(i).getLiteral().getStringValue();
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for ArrayLiteralTransformFunction: " + _dataType + ", literal contexts: "
                + Arrays.toString(literalContexts.toArray()));
    }
  }

  public int[] getIntArrayLiteral() {
    return _intArrayLiteral;
  }

  public long[] getLongArrayLiteral() {
    return _longArrayLiteral;
  }

  public float[] getFloatArrayLiteral() {
    return _floatArrayLiteral;
  }

  public double[] getDoubleArrayLiteral() {
    return _doubleArrayLiteral;
  }

  public String[] getStringArrayLiteral() {
    return _stringArrayLiteral;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_dataType, false, false);
  }

  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[][] intArrayResult = _intArrayResult;
    if (intArrayResult == null || intArrayResult.length < numDocs) {
      intArrayResult = new int[numDocs][];
      Arrays.fill(intArrayResult, _intArrayLiteral);
      _intArrayResult = intArrayResult;
    }
    return intArrayResult;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    long[][] longArrayResult = _longArrayResult;
    if (longArrayResult == null || longArrayResult.length < numDocs) {
      longArrayResult = new long[numDocs][];
      Arrays.fill(longArrayResult, _longArrayLiteral);
      _longArrayResult = longArrayResult;
    }
    return longArrayResult;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    float[][] floatArrayResult = _floatArrayResult;
    if (floatArrayResult == null || floatArrayResult.length < numDocs) {
      floatArrayResult = new float[numDocs][];
      Arrays.fill(floatArrayResult, _floatArrayLiteral);
      _floatArrayResult = floatArrayResult;
    }
    return floatArrayResult;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    double[][] doubleArrayResult = _doubleArrayResult;
    if (doubleArrayResult == null || doubleArrayResult.length < numDocs) {
      doubleArrayResult = new double[numDocs][];
      Arrays.fill(doubleArrayResult, _doubleArrayLiteral);
      _doubleArrayResult = doubleArrayResult;
    }
    return doubleArrayResult;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    String[][] stringArrayResult = _stringArrayResult;
    if (stringArrayResult == null || stringArrayResult.length < numDocs) {
      stringArrayResult = new String[numDocs][];
      Arrays.fill(stringArrayResult, _stringArrayLiteral);
      _stringArrayResult = stringArrayResult;
    }
    return stringArrayResult;
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    // Treat all unknown type values as null regardless of the value.
    if (_dataType != DataType.UNKNOWN) {
      return null;
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, length);
    return bitmap;
  }
}
