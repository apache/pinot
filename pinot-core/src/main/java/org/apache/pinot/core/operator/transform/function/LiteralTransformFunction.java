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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The <code>LiteralTransformFunction</code> class is a special transform function which is a wrapper on top of a
 * LITERAL. The data type is inferred from the literal string.
 * TODO: Preserve the type of the literal instead of inferring the type from the string
 */
public class LiteralTransformFunction implements TransformFunction {
  // TODO: Deprecate the string representation of literal.
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

  public Object getLiteral() {
    return _literal;
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_dataType, true, false);
  }

  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToDictIdsSV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
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
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    long[] longResult = _longResult;
    if (longResult == null || longResult.length < numDocs) {
      longResult = new long[numDocs];
      if (_dataType != DataType.TIMESTAMP) {
        if (_longLiteral != 0) {
          // TODO: Handle null literal
          Arrays.fill(longResult, _longLiteral);
        }
      } else {
        Arrays.fill(longResult, Timestamp.valueOf(_literal.toString()).getTime());
      }
      _longResult = longResult;
    }
    return longResult;
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
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
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
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
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    BigDecimal[] bigDecimalResult = _bigDecimalResult;
    if (bigDecimalResult == null || bigDecimalResult.length < numDocs) {
      bigDecimalResult = new BigDecimal[numDocs];
      Arrays.fill(bigDecimalResult, _bigDecimalLiteral);
      _bigDecimalResult = bigDecimalResult;
    }
    return bigDecimalResult;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    String[] stringResult = _stringResult;
    if (stringResult == null || stringResult.length < numDocs) {
      stringResult = new String[numDocs];
      // TODO: Handle null literal
      Arrays.fill(stringResult, _literal.toString());
      _stringResult = stringResult;
    }
    return stringResult;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    byte[][] bytesResult = _bytesResult;
    if (bytesResult == null || bytesResult.length < numDocs) {
      bytesResult = new byte[numDocs][];
      // TODO: Handle null literal
      Arrays.fill(bytesResult, BytesUtils.toBytes(_literal.toString()));
      _bytesResult = bytesResult;
    }
    return bytesResult;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }
}
