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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * As for Gapfilling Function, all raw data will be retrieved from the pinot
 * server and merged on the pinot broker. The data will be in {@link DataTable}
 * format.
 * As part of Gapfilling Function execution plan, the aggregation function will
 * work on the merged data on pinot broker. The aggregation function only takes
 * the {@link BlockValSet} format.
 * This is the Helper class to convert the data from {@link DataTable} to the
 * block of values {@link BlockValSet} which used as input to the aggregation
 * function.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ColumnDataToBlockValSetConverter implements BlockValSet {

  private final FieldSpec.DataType _dataType;
  private final List<Object[]> _rows;
  private final int _columnIndex;

  public ColumnDataToBlockValSetConverter(DataSchema.ColumnDataType columnDataType, List<Object[]> rows,
      int columnIndex) {
    _dataType = columnDataType.toDataType();
    _rows = rows;
    _columnIndex = columnIndex;
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
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int[] getIntValuesSV() {
    if (_dataType == FieldSpec.DataType.INT) {
      int[] result = new int[_rows.size()];
      for (int i = 0; i < result.length; i++) {
        result[i] = (Integer) _rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public long[] getLongValuesSV() {
    if (_dataType == FieldSpec.DataType.LONG) {
      long[] result = new long[_rows.size()];
      for (int i = 0; i < result.length; i++) {
        result[i] = (Long) _rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public float[] getFloatValuesSV() {
    if (_dataType == FieldSpec.DataType.FLOAT) {
      float[] result = new float[_rows.size()];
      for (int i = 0; i < result.length; i++) {
        result[i] = (Float) _rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public double[] getDoubleValuesSV() {
    if (_dataType == FieldSpec.DataType.DOUBLE) {
      double[] result = new double[_rows.size()];
      for (int i = 0; i < result.length; i++) {
        result[i] = (Double) _rows.get(i)[_columnIndex];
      }
      return result;
    } else if (_dataType == FieldSpec.DataType.INT) {
      double[] result = new double[_rows.size()];
      for (int i = 0; i < result.length; i++) {
        result[i] = ((Integer) _rows.get(i)[_columnIndex]).doubleValue();
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public String[] getStringValuesSV() {
    if (_dataType == FieldSpec.DataType.STRING) {
      String[] result = new String[_rows.size()];
      for (int i = 0; i < result.length; i++) {
        result[i] = (String) _rows.get(i)[_columnIndex];
      }
      return result;
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public byte[][] getBytesValuesSV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException("Not supported");
  }
}
