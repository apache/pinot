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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * This class serves as a block level cache for column dictionary Ids and values. Using this class can prevent fetching
 * data for the same column multiple times. This class allocate resources on demand, and reuse them as much as possible
 * to prevent garbage collection.
 */
@SuppressWarnings("Duplicates")
public class DataBlockCache {
  private final DataFetcher _dataFetcher;

  // Mark whether data have been fetched, need to be cleared in initNewBlock()
  private final Set<String> _columnDictIdLoaded = new HashSet<>();
  private final Set<ColumnTypePair> _columnValueLoaded = new HashSet<>();
  private final Set<String> _columnNumValuesLoaded = new HashSet<>();

  // Buffer for data
  private final Map<String, Object> _dictIdsMap = new HashMap<>();
  private final Map<ColumnTypePair, Object> _valuesMap = new HashMap<>();
  private final Map<String, int[]> _numValuesMap = new HashMap<>();

  private int[] _docIds;
  private int _length;

  public DataBlockCache(DataFetcher dataFetcher) {
    _dataFetcher = dataFetcher;
  }

  /**
   * Init the data block cache with document Ids for a new block. This method should be called before fetching data for
   * any specific block.
   *
   * @param docIds Document Ids buffer
   * @param length Number of document Ids
   */
  public void initNewBlock(int[] docIds, int length) {
    _docIds = docIds;
    _length = length;

    _columnDictIdLoaded.clear();
    _columnValueLoaded.clear();
    _columnNumValuesLoaded.clear();
  }

  /**
   * Returns the number of documents within the current block.
   *
   * @return Number of documents within the current block
   */
  public int getNumDocs() {
    return _length;
  }

  /**
   * SINGLE-VALUED COLUMN API
   */

  /**
   * Get the dictionary Ids for a single-valued column.
   *
   * @param column Column name
   * @return Array of dictionary Ids
   */
  public int[] getDictIdsForSVColumn(String column) {
    int[] dictIds = (int[]) _dictIdsMap.get(column);
    if (_columnDictIdLoaded.add(column)) {
      if (dictIds == null) {
        dictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _dictIdsMap.put(column, dictIds);
      }
      _dataFetcher.fetchDictIds(column, _docIds, _length, dictIds);
    }
    return dictIds;
  }

  /**
   * Get the int values for a single-valued column.
   *
   * @param column Column name
   * @return Array of int values
   */
  public int[] getIntValuesForSVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.INT);
    int[] intValues = (int[]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (intValues == null) {
        intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _valuesMap.put(key, intValues);
      }
      _dataFetcher.fetchIntValues(column, _docIds, _length, intValues);
    }
    return intValues;
  }

  /**
   * Get the long values for a single-valued column.
   *
   * @param column Column name
   * @return Array of long values
   */
  public long[] getLongValuesForSVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.LONG);
    long[] longValues = (long[]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (longValues == null) {
        longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _valuesMap.put(key, longValues);
      }
      _dataFetcher.fetchLongValues(column, _docIds, _length, longValues);
    }
    return longValues;
  }

  /**
   * Get the float values for a single-valued column.
   *
   * @param column Column name
   * @return Array of float values
   */
  public float[] getFloatValuesForSVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.FLOAT);
    float[] floatValues = (float[]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (floatValues == null) {
        floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _valuesMap.put(key, floatValues);
      }
      _dataFetcher.fetchFloatValues(column, _docIds, _length, floatValues);
    }
    return floatValues;
  }

  /**
   * Get the double values for a single-valued column.
   *
   * @param column Column name
   * @return Array of double values
   */
  public double[] getDoubleValuesForSVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.DOUBLE);
    double[] doubleValues = (double[]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (doubleValues == null) {
        doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _valuesMap.put(key, doubleValues);
      }
      _dataFetcher.fetchDoubleValues(column, _docIds, _length, doubleValues);
    }
    return doubleValues;
  }

  /**
   * Get the string values for a single-valued column.
   *
   * @param column Column name
   * @return Array of string values
   */
  public String[] getStringValuesForSVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.STRING);
    String[] stringValues = (String[]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (stringValues == null) {
        stringValues = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _valuesMap.put(key, stringValues);
      }
      _dataFetcher.fetchStringValues(column, _docIds, _length, stringValues);
    }
    return stringValues;
  }

  /**
   * Get byte[] values for the given single-valued column.
   *
   * @param column Column to read
   * @return byte[] for the column
   */
  public byte[][] getBytesValuesForSVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.BYTES);
    byte[][] bytesValues = (byte[][]) _valuesMap.get(key);

    if (_columnValueLoaded.add(key)) {
      if (bytesValues == null) {
        bytesValues = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _valuesMap.put(key, bytesValues);
      }
      _dataFetcher.fetchBytesValues(column, _docIds, _length, bytesValues);
    }
    return bytesValues;
  }

  /**
   * MULTI-VALUED COLUMN API
   */

  /**
   * Get the dictionary Ids for a multi-valued column.
   *
   * @param column Column name
   * @return Array of dictionary Ids
   */
  public int[][] getDictIdsForMVColumn(String column) {
    int[][] dictIds = (int[][]) _dictIdsMap.get(column);
    if (_columnDictIdLoaded.add(column)) {
      if (dictIds == null) {
        dictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _dictIdsMap.put(column, dictIds);
      }
      _dataFetcher.fetchDictIds(column, _docIds, _length, dictIds);
    }
    return dictIds;
  }

  /**
   * Get the int values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of int values
   */
  public int[][] getIntValuesForMVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.INT);
    int[][] intValues = (int[][]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (intValues == null) {
        intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _valuesMap.put(key, intValues);
      }
      _dataFetcher.fetchIntValues(column, _docIds, _length, intValues);
    }
    return intValues;
  }

  /**
   * Get the long values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of long values
   */
  public long[][] getLongValuesForMVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.LONG);
    long[][] longValues = (long[][]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (longValues == null) {
        longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _valuesMap.put(key, longValues);
      }
      _dataFetcher.fetchLongValues(column, _docIds, _length, longValues);
    }
    return longValues;
  }

  /**
   * Get the float values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of float values
   */
  public float[][] getFloatValuesForMVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.FLOAT);
    float[][] floatValues = (float[][]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (floatValues == null) {
        floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _valuesMap.put(key, floatValues);
      }
      _dataFetcher.fetchFloatValues(column, _docIds, _length, floatValues);
    }
    return floatValues;
  }

  /**
   * Get the double values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of double values
   */
  public double[][] getDoubleValuesForMVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.DOUBLE);
    double[][] doubleValues = (double[][]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (doubleValues == null) {
        doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _valuesMap.put(key, doubleValues);
      }
      _dataFetcher.fetchDoubleValues(column, _docIds, _length, doubleValues);
    }
    return doubleValues;
  }

  /**
   * Get the string values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of string values
   */
  public String[][] getStringValuesForMVColumn(String column) {
    ColumnTypePair key = new ColumnTypePair(column, FieldSpec.DataType.STRING);
    String[][] stringValues = (String[][]) _valuesMap.get(key);
    if (_columnValueLoaded.add(key)) {
      if (stringValues == null) {
        stringValues = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _valuesMap.put(key, stringValues);
      }
      _dataFetcher.fetchStringValues(column, _docIds, _length, stringValues);
    }
    return stringValues;
  }

  /**
   * Get the number of values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of number of values
   */
  public int[] getNumValuesForMVColumn(String column) {
    int[] numValues = _numValuesMap.get(column);
    if (_columnNumValuesLoaded.add(column)) {
      if (numValues == null) {
        numValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _numValuesMap.put(column, numValues);
      }
      _dataFetcher.fetchNumValues(column, _docIds, _length, numValues);
    }
    return numValues;
  }

  /**
   * Helper class to store pair of column name and data type.
   */
  private static class ColumnTypePair {
    final String _column;
    final FieldSpec.DataType _dataType;

    ColumnTypePair(@Nonnull String column, @Nonnull FieldSpec.DataType dataType) {
      _column = column;
      _dataType = dataType;
    }

    @Override
    public int hashCode() {
      return EqualityUtils.hashCodeOf(_column.hashCode(), _dataType.hashCode());
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
      ColumnTypePair that = (ColumnTypePair) obj;
      return _column.equals(that._column) && _dataType == that._dataType;
    }
  }
}
