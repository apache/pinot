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
package org.apache.pinot.core.operator.docvalsets;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * A {@link BlockValSet} implementation backed by row major data with a filter column (BOOLEAN type).
 *
 * TODO: Support MV
 */
public class FilteredRowBasedBlockValSet implements BlockValSet {
  private final DataType _dataType;
  private final DataType _storedType;
  private final List<Object[]> _rows;
  private final int _colId;
  private final int _numMatchedRows;
  private final RoaringBitmap _matchedBitmap;
  private final RoaringBitmap _matchedNullBitmap;

  public FilteredRowBasedBlockValSet(ColumnDataType columnDataType, List<Object[]> rows, int colId, int numMatchedRows,
      RoaringBitmap matchedBitmap, boolean nullHandlingEnabled) {
    _dataType = columnDataType.toDataType();
    _storedType = _dataType.getStoredType();
    _rows = rows;
    _colId = colId;
    _numMatchedRows = numMatchedRows;
    _matchedBitmap = matchedBitmap;

    if (nullHandlingEnabled) {
      RoaringBitmap matchedNullBitmap = new RoaringBitmap();
      PeekableIntIterator iterator = matchedBitmap.getIntIterator();
      for (int i = 0; i < numMatchedRows; i++) {
        int rowId = iterator.next();
        if (rows.get(rowId)[colId] == null) {
          matchedNullBitmap.add(i);
        }
      }
      _matchedNullBitmap = !matchedNullBitmap.isEmpty() ? matchedNullBitmap : null;
    } else {
      _matchedNullBitmap = null;
    }
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return _matchedNullBitmap;
  }

  @Override
  public DataType getValueType() {
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
    Preconditions.checkState(_dataType == DataType.UNKNOWN || _storedType.isNumeric() || _storedType == DataType.STRING,
        "Cannot read int values from data type: %s", _dataType);
    int[] values = new int[_numMatchedRows];
    if (_numMatchedRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (_matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      if (_storedType.isNumeric()) {
        values[matchedRowId] = ((Number) _rows.get(rowId)[_colId]).intValue();
      } else {
        values[matchedRowId] = Integer.parseInt((String) _rows.get(rowId)[_colId]);
      }
    }
    return values;
  }

  @Override
  public long[] getLongValuesSV() {
    Preconditions.checkState(_dataType == DataType.UNKNOWN || _storedType.isNumeric() || _storedType == DataType.STRING,
        "Cannot read long values from data type: %s", _dataType);
    long[] values = new long[_numMatchedRows];
    if (_numMatchedRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (_matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      if (_storedType.isNumeric()) {
        values[matchedRowId] = ((Number) _rows.get(rowId)[_colId]).longValue();
      } else {
        values[matchedRowId] = Long.parseLong((String) _rows.get(rowId)[_colId]);
      }
    }
    return values;
  }

  @Override
  public float[] getFloatValuesSV() {
    Preconditions.checkState(_dataType == DataType.UNKNOWN || _storedType.isNumeric() || _storedType == DataType.STRING,
        "Cannot read float values from data type: %s", _dataType);
    float[] values = new float[_numMatchedRows];
    if (_numMatchedRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (_matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      if (_storedType.isNumeric()) {
        values[matchedRowId] = ((Number) _rows.get(rowId)[_colId]).floatValue();
      } else {
        values[matchedRowId] = Float.parseFloat((String) _rows.get(rowId)[_colId]);
      }
    }
    return values;
  }

  @Override
  public double[] getDoubleValuesSV() {
    Preconditions.checkState(_dataType == DataType.UNKNOWN || _storedType.isNumeric() || _storedType == DataType.STRING,
        "Cannot read double values from data type: %s", _dataType);
    double[] values = new double[_numMatchedRows];
    if (_numMatchedRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (_matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId)) {
        continue;
      }
      if (_storedType.isNumeric()) {
        values[matchedRowId] = ((Number) _rows.get(rowId)[_colId]).doubleValue();
      } else {
        values[matchedRowId] = Double.parseDouble((String) _rows.get(rowId)[_colId]);
      }
    }
    return values;
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    BigDecimal[] values = new BigDecimal[_numMatchedRows];
    if (_numMatchedRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BIG_DECIMAL);
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      if (_matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId)) {
        values[matchedRowId] = NullValuePlaceHolder.BIG_DECIMAL;
        continue;
      }
      switch (_storedType) {
        case INT:
        case LONG:
          values[matchedRowId] = BigDecimal.valueOf(((Number) _rows.get(rowId)[_colId]).longValue());
          break;
        case FLOAT:
        case DOUBLE:
        case STRING:
          values[matchedRowId] = new BigDecimal(_rows.get(rowId)[_colId].toString());
          break;
        case BIG_DECIMAL:
          values[matchedRowId] = (BigDecimal) _rows.get(rowId)[_colId];
          break;
        case BYTES:
          values[matchedRowId] = BigDecimalUtils.deserialize((ByteArray) _rows.get(rowId)[_colId]);
          break;
        default:
          throw new IllegalStateException("Cannot read BigDecimal values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public String[] getStringValuesSV() {
    String[] values = new String[_numMatchedRows];
    if (_numMatchedRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.STRING);
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      boolean isNull = _matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId);
      values[matchedRowId] = !isNull ? _rows.get(rowId)[_colId].toString() : NullValuePlaceHolder.STRING;
    }
    return values;
  }

  @Override
  public byte[][] getBytesValuesSV() {
    byte[][] values = new byte[_numMatchedRows][];
    if (_numMatchedRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BYTES);
      return values;
    }
    PeekableIntIterator iterator = _matchedBitmap.getIntIterator();
    for (int matchedRowId = 0; matchedRowId < _numMatchedRows; matchedRowId++) {
      int rowId = iterator.next();
      boolean isNull = _matchedNullBitmap != null && _matchedNullBitmap.contains(matchedRowId);
      values[matchedRowId] = !isNull ? ((ByteArray) _rows.get(rowId)[_colId]).getBytes() : NullValuePlaceHolder.BYTES;
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
