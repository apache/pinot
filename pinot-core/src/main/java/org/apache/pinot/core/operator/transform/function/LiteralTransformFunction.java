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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>LiteralTransformFunction</code> class is a special transform function which is a wrapper on top of a
 * LITERAL. The data type is inferred from the literal string.
 */
public class LiteralTransformFunction implements TransformFunction {
  public static final String FUNCTION_NAME = "literal";

  private final LiteralContext _literalContext;

  // literals may be shared but values are intentionally not volatile as assignment races are benign
  private int[] _intResult;
  private long[] _longResult;
  private float[] _floatResult;
  private double[] _doubleResult;
  private BigDecimal[] _bigDecimalResult;
  private String[] _stringResult;
  private byte[][] _bytesResult;

  public LiteralTransformFunction(LiteralContext literalContext) {
    _literalContext = literalContext;
  }

  public boolean getBooleanLiteral() {
    return _literalContext.getBooleanValue();
  }

  public int getIntLiteral() {
    return _literalContext.getIntValue();
  }

  public long getLongLiteral() {
    return _literalContext.getLongValue();
  }

  public float getFloatLiteral() {
    return _literalContext.getFloatValue();
  }

  public double getDoubleLiteral() {
    return _literalContext.getDoubleValue();
  }

  public BigDecimal getBigDecimalLiteral() {
    return _literalContext.getBigDecimalValue();
  }

  public String getStringLiteral() {
    return _literalContext.getStringValue();
  }

  public byte[] getBytesLiteral() {
    return _literalContext.getBytesValue();
  }

  public boolean isNull() {
    return _literalContext.isNull();
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
    return new TransformResultMetadata(_literalContext.getType(), true, false);
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
      int intValue = getIntLiteral();
      intResult = new int[numDocs];
      if (intValue != 0) {
        Arrays.fill(intResult, intValue);
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
      long longValue = getLongLiteral();
      longResult = new long[numDocs];
      if (longValue != 0) {
        Arrays.fill(longResult, longValue);
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
      float floatValue = getFloatLiteral();
      floatResult = new float[numDocs];
      if (floatValue != 0) {
        Arrays.fill(floatResult, floatValue);
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
      double doubleValue = getDoubleLiteral();
      doubleResult = new double[numDocs];
      if (doubleValue != 0) {
        Arrays.fill(doubleResult, doubleValue);
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
      Arrays.fill(bigDecimalResult, getBigDecimalLiteral());
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
      Arrays.fill(bytesResult, getBytesLiteral());
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

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    if (!isNull()) {
      return null;
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, length);
    return bitmap;
  }
}
