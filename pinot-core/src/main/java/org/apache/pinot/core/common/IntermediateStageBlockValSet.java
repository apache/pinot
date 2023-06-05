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
package org.apache.pinot.core.common;

import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;

/**
 * In the multistage engine, the leaf stage servers process the data in columnar fashion. By the time the
 * intermediate stage receives the projected column, they are converted to a row based format. This class provides
 * the capability to convert the row based represenation into blocks so that they can be used to process
 * aggregations.
 * TODO: Support MV
 */
public class IntermediateStageBlockValSet implements BlockValSet {
  private final FieldSpec.DataType _dataType;
  private final PinotDataType _pinotDataType;
  private final List<Object> _values;
  private final RoaringBitmap _nullBitMap;
  private boolean _nullBitMapSet;

  public IntermediateStageBlockValSet(DataSchema.ColumnDataType columnDataType, List<Object> values) {
    _dataType = columnDataType.toDataType();
    _pinotDataType = PinotDataType.getPinotDataTypeForExecution(columnDataType);
    _values = values;
    _nullBitMap = new RoaringBitmap();
  }

  /**
   * Returns a bitmap of indices where null values are found.
   */
  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    if (!_nullBitMapSet) {
      if (_values == null) {
        return _nullBitMap;
      }

      for (int i = 0; i < _values.size(); i++) {
        if (_values.get(i) == null) {
          _nullBitMap.add(i);
        }
      }
      _nullBitMapSet = true;
    }
    return _nullBitMap;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    // TODO: Needs to be changed when we start supporting MV in multistage
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
    if (_values == null) {
      return null;
    }

    int length = _values.size();
    int[] values = new int[length];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toInt(value);
      }
    }
    return values;
  }

  @Override
  public long[] getLongValuesSV() {
    if (_values == null) {
      return null;
    }

    int length = _values.size();
    long[] values = new long[length];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toLong(value);
      }
    }

    return values;
  }

  @Override
  public float[] getFloatValuesSV() {
    if (_values == null) {
      return null;
    }
    int length = _values.size();
    float[] values = new float[length];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toFloat(value);
      }
    }

    return values;
  }

  @Override
  public double[] getDoubleValuesSV() {
    if (_values == null) {
      return null;
    }
    int length = _values.size();
    double[] values = new double[length];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toDouble(value);
      }
    }

    return values;
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    if (_values == null) {
      return null;
    }
    int length = _values.size();
    BigDecimal[] values = new BigDecimal[length];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toBigDecimal(value);
      }
    }
    return values;
  }

  @Override
  public String[] getStringValuesSV() {
    if (_values == null) {
      return null;
    }
    int length = _values.size();
    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toString(value);
      }
    }
    return values;
  }

  @Override
  public byte[][] getBytesValuesSV() {
    if (_values == null) {
      return null;
    }
    int length = _values.size();
    byte[][] values = new byte[length][];
    for (int i = 0; i < length; i++) {
      Object value = _values.get(i);
      if (value != null) {
        values[i] = _pinotDataType.toBytes(value);
      }
    }
    return values;
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
