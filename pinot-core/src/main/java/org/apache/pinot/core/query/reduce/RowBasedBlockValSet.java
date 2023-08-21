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
package org.apache.pinot.core.query.reduce;

import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.RoaringBitmap;


/**
 * When the data is retrieved from pinot servers and merged on the broker, it
 * will become row based data. If we want to apply Transformation or Aggregation,
 * we need {@link BlockValSet} format data. This class is used to provide
 * {@link BlockValSet} interface wrapping around row based data.
 *
 * TODO: We need add support for BYTES and MV
 */
public class RowBasedBlockValSet implements BlockValSet {

  private final FieldSpec.DataType _dataType;
  private final PinotDataType _pinotDataType;
  private final List<Object[]> _rows;
  private final int _columnIndex;

  public RowBasedBlockValSet(DataSchema.ColumnDataType columnDataType, List<Object[]> rows,
      int columnIndex) {
    _dataType = columnDataType.toDataType();
    _pinotDataType = PinotDataType.getPinotDataTypeForExecution(columnDataType);
    _rows = rows;
    _columnIndex = columnIndex;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    // TODO: The assumption for now is that the rows in RowBasedBlockValSet contain non-null values.
    //  Update to pass nullBitmap in constructor if rows have null values. Alternatively, compute nullBitmap on the fly.
    return null;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    int length = _rows.size();
    int[] values = new int[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _rows.get(i)[_columnIndex]).intValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Integer.parseInt((String) _rows.get(i)[_columnIndex]);
      }
    } else {
      throw new IllegalStateException("Cannot read int values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public long[] getLongValuesSV() {
    int length = _rows.size();
    long[] values = new long[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _rows.get(i)[_columnIndex]).longValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Long.parseLong((String) _rows.get(i)[_columnIndex]);
      }
    } else {
      throw new IllegalStateException("Cannot read long values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public float[] getFloatValuesSV() {
    int length = _rows.size();
    float[] values = new float[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _rows.get(i)[_columnIndex]).floatValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Float.parseFloat((String) _rows.get(i)[_columnIndex]);
      }
    } else {
      throw new IllegalStateException("Cannot read float values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public double[] getDoubleValuesSV() {
    int length = _rows.size();
    double[] values = new double[length];
    if (_dataType.isNumeric()) {
      for (int i = 0; i < length; i++) {
        values[i] = ((Number) _rows.get(i)[_columnIndex]).doubleValue();
      }
    } else if (_dataType == FieldSpec.DataType.STRING) {
      for (int i = 0; i < length; i++) {
        values[i] = Double.parseDouble((String) _rows.get(i)[_columnIndex]);
      }
    } else {
      throw new IllegalStateException("Cannot read double values from data type: " + _dataType);
    }
    return values;
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    int length = _rows.size();
    BigDecimal[] values = new BigDecimal[length];
    for (int i = 0; i < length; i++) {
      values[i] = _pinotDataType.toBigDecimal(_rows.get(i)[_columnIndex]);
    }
    return values;
  }

  @Override
  public Vector[] getVectorValuesSV() {
    int length = _rows.size();
    Vector[] values = new Vector[length];
    for (int i = 0; i < length; i++) {
      values[i] = _pinotDataType.toVector(_rows.get(i)[_columnIndex]);
    }
    return values;
  }

  @Override
  public String[] getStringValuesSV() {
    int length = _rows.size();
    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      values[i] = _rows.get(i)[_columnIndex].toString();
    }
    return values;
  }

  @Override
  public byte[][] getBytesValuesSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException();
  }
}
