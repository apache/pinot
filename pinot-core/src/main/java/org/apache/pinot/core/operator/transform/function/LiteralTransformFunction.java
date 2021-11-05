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

import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.math.NumberUtils;
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
  private final String _literal;
  private final DataType _dataType;

  // literals may be shared but values are intentionally not volatile as assignment races are benign
  private int[] _intResult;
  private long[] _longResult;
  private float[] _floatResult;
  private double[] _doubleResult;
  private String[] _stringResult;
  private byte[][] _bytesResult;

  public LiteralTransformFunction(String literal) {
    _literal = literal;
    _dataType = inferLiteralDataType(literal);
  }

  @VisibleForTesting
  static DataType inferLiteralDataType(String literal) {
    // Try to interpret the literal as number
    try {
      Number number = NumberUtils.createNumber(literal);
      if (number instanceof Integer) {
        return DataType.INT;
      } else if (number instanceof Long) {
        return DataType.LONG;
      } else if (number instanceof Float) {
        return DataType.FLOAT;
      } else if (number instanceof Double) {
        return DataType.DOUBLE;
      } else {
        return DataType.STRING;
      }
    } catch (Exception e) {
      // Ignored
    }

    // Try to interpret the literal as BOOLEAN
    // NOTE: Intentionally use equals() instead of equalsIgnoreCase() here because boolean literal will always be parsed
    //       into lowercase string. We don't want to parse string "TRUE" as boolean.
    if (literal.equals("true") || literal.equals("false")) {
      return DataType.BOOLEAN;
    }

    // Try to interpret the literal as TIMESTAMP
    try {
      Timestamp.valueOf(literal);
      return DataType.TIMESTAMP;
    } catch (Exception e) {
      // Ignored
    }

    return DataType.STRING;
  }

  public String getLiteral() {
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
        Arrays.fill(intResult, new BigDecimal(_literal).intValue());
      } else {
        Arrays.fill(intResult, _literal.equals("true") ? 1 : 0);
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
        Arrays.fill(longResult, new BigDecimal(_literal).longValue());
      } else {
        Arrays.fill(longResult, Timestamp.valueOf(_literal).getTime());
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
      Arrays.fill(floatResult, new BigDecimal(_literal).floatValue());
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
      Arrays.fill(doubleResult, new BigDecimal(_literal).doubleValue());
      _doubleResult = doubleResult;
    }
    return _doubleResult;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    String[] stringResult = _stringResult;
    if (stringResult == null || stringResult.length < numDocs) {
      stringResult = new String[numDocs];
      Arrays.fill(stringResult, _literal);
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
      Arrays.fill(bytesResult, BytesUtils.toBytes(_literal));
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
}
