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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>LiteralTransformFunction</code> class is a special transform function which is a wrapper on top of a
 * LITERAL. The data type is inferred from the literal string.
 */
public class LiteralTransformFunction implements TransformFunction {
  public static final String FUNCTION_NAME = "literal";

  private final Object _literal;
  private final DataType _dataType;
  private final int _intLiteral;
  private final long _longLiteral;
  private final float _floatLiteral;
  private final double _doubleLiteral;
  private final BigDecimal _bigDecimalLiteral;

  // literals may be shared but values are intentionally not volatile as assignment races are benign
  private int[] _intResult;
  private long[] _longResult;
  private float[] _floatResult;
  private double[] _doubleResult;
  private BigDecimal[] _bigDecimalResult;
  private String[] _stringResult;
  private byte[][] _bytesResult;

  public LiteralTransformFunction(LiteralContext literalContext) {
    Preconditions.checkNotNull(literalContext);
    _literal = literalContext.getValue();
    _dataType = literalContext.getType();
    _bigDecimalLiteral = literalContext.getBigDecimalValue();
    _intLiteral = _bigDecimalLiteral.intValue();
    _longLiteral = _bigDecimalLiteral.longValue();
    _floatLiteral = _bigDecimalLiteral.floatValue();
    _doubleLiteral = _bigDecimalLiteral.doubleValue();
  }

  public boolean getBooleanLiteral() {
    return BooleanUtils.toBoolean(_literal);
  }

  public int getIntLiteral() {
    return _intLiteral;
  }

  public long getLongLiteral() {
    return _longLiteral;
  }

  public float getFloatLiteral() {
    return _floatLiteral;
  }

  public double getDoubleLiteral() {
    return _doubleLiteral;
  }

  public BigDecimal getBigDecimalLiteral() {
    return _bigDecimalLiteral;
  }

  public String getStringLiteral() {
    return String.valueOf(_literal);
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
    return new TransformResultMetadata(_dataType, true, false);
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
    int numDocs = valueBlock.getNumDocs();
    int[] intResult = _intResult;
    if (intResult == null || intResult.length < numDocs) {
      intResult = new int[numDocs];
      if (_dataType != DataType.BOOLEAN) {
        if (_intLiteral != 0) {
          Arrays.fill(intResult, _intLiteral);
        }
      } else {
        Arrays.fill(intResult, _intLiteral);
      }
      _intResult = intResult;
    }
    return intResult;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    long[] longResult = _longResult;
    if (longResult == null || longResult.length < numDocs) {
      longResult = new long[numDocs];
      if (_dataType != DataType.TIMESTAMP) {
        if (_longLiteral != 0) {
          Arrays.fill(longResult, _longLiteral);
        }
      } else {
        Arrays.fill(longResult, Timestamp.valueOf(getStringLiteral()).getTime());
      }
      _longResult = longResult;
    }
    return longResult;
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    float[] floatResult = _floatResult;
    if (floatResult == null || floatResult.length < numDocs) {
      floatResult = new float[numDocs];
      if (_floatLiteral != 0F) {
        Arrays.fill(floatResult, _floatLiteral);
      }
      _floatResult = floatResult;
    }
    return floatResult;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    double[] doubleResult = _doubleResult;
    if (doubleResult == null || doubleResult.length < numDocs) {
      doubleResult = new double[numDocs];
      if (_doubleLiteral != 0) {
        Arrays.fill(doubleResult, _doubleLiteral);
      }
      _doubleResult = doubleResult;
    }
    return doubleResult;
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    BigDecimal[] bigDecimalResult = _bigDecimalResult;
    if (bigDecimalResult == null || bigDecimalResult.length < numDocs) {
      bigDecimalResult = new BigDecimal[numDocs];
      Arrays.fill(bigDecimalResult, _bigDecimalLiteral);
      _bigDecimalResult = bigDecimalResult;
    }
    return bigDecimalResult;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    String[] stringResult = _stringResult;
    if (stringResult == null || stringResult.length < numDocs) {
      stringResult = new String[numDocs];
      Arrays.fill(stringResult, getStringLiteral());
      _stringResult = stringResult;
    }
    return stringResult;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    byte[][] bytesResult = _bytesResult;
    if (bytesResult == null || bytesResult.length < numDocs) {
      bytesResult = new byte[numDocs][];
      // TODO: Handle null literal
      Arrays.fill(bytesResult, BytesUtils.toBytes(getStringLiteral()));
      _bytesResult = bytesResult;
    }
    return bytesResult;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
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

  // checks whether the literal is null or not.
  public boolean isNull() {
    return _dataType == DataType.UNKNOWN;
  }
}
