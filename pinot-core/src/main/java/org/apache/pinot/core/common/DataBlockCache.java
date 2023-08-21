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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.evaluator.TransformEvaluator;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.Vector;


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
  private final Map<FieldSpec.DataType, Set<String>> _columnValueLoaded = new EnumMap<>(FieldSpec.DataType.class);
  private final Set<String> _columnNumValuesLoaded = new HashSet<>();

  // Buffer for data
  private final Map<String, Object> _dictIdsMap = new HashMap<>();
  private final Map<FieldSpec.DataType, Map<String, Object>> _valuesMap = new HashMap<>();
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
    if (length > _length) {
      _dictIdsMap.clear();
      _valuesMap.clear();
      _numValuesMap.clear();
    }
    _length = length;
    _columnDictIdLoaded.clear();
    for (Set<String> columns : _columnValueLoaded.values()) {
      columns.clear();
    }
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
   * Returns the document ids within the current block.
   *
   * @return Document ids within the current block.
   */
  public int[] getDocIds() {
    return _docIds;
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
        dictIds = new int[_length];
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
    int[] intValues = getValues(FieldSpec.DataType.INT, column);
    if (markLoaded(FieldSpec.DataType.INT, column)) {
      if (intValues == null) {
        intValues = new int[_length];
        putValues(FieldSpec.DataType.INT, column, intValues);
      }
      _dataFetcher.fetchIntValues(column, _docIds, _length, intValues);
    }
    return intValues;
  }

  /**
   * Get the int values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, int[] buffer) {
    _dataFetcher.fetchIntValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the long values for a single-valued column.
   *
   * @param column Column name
   * @return Array of long values
   */
  public long[] getLongValuesForSVColumn(String column) {
    long[] longValues = getValues(FieldSpec.DataType.LONG, column);
    if (markLoaded(FieldSpec.DataType.LONG, column)) {
      if (longValues == null) {
        longValues = new long[_length];
        putValues(FieldSpec.DataType.LONG, column, longValues);
      }
      _dataFetcher.fetchLongValues(column, _docIds, _length, longValues);
    }
    return longValues;
  }

  /**
   * Get the long values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, long[] buffer) {
    _dataFetcher.fetchLongValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the float values for a single-valued column.
   *
   * @param column Column name
   * @return Array of float values
   */
  public float[] getFloatValuesForSVColumn(String column) {
    float[] floatValues = getValues(FieldSpec.DataType.FLOAT, column);
    if (markLoaded(FieldSpec.DataType.FLOAT, column)) {
      if (floatValues == null) {
        floatValues = new float[_length];
        putValues(FieldSpec.DataType.FLOAT, column, floatValues);
      }
      _dataFetcher.fetchFloatValues(column, _docIds, _length, floatValues);
    }
    return floatValues;
  }

  /**
   * Get the float values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, float[] buffer) {
    _dataFetcher.fetchFloatValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the double values for a single-valued column.
   *
   * @param column Column name
   * @return Array of double values
   */
  public double[] getDoubleValuesForSVColumn(String column) {
    double[] doubleValues = getValues(FieldSpec.DataType.DOUBLE, column);
    if (markLoaded(FieldSpec.DataType.DOUBLE, column)) {
      if (doubleValues == null) {
        doubleValues = new double[_length];
        putValues(FieldSpec.DataType.DOUBLE, column, doubleValues);
      }
      _dataFetcher.fetchDoubleValues(column, _docIds, _length, doubleValues);
    }
    return doubleValues;
  }

  /**
   * Get the double values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, double[] buffer) {
    _dataFetcher.fetchDoubleValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the BigDecimal values for a single-valued column.
   *
   * @param column Column name
   * @return Array of BigDecimal values
   */
  public BigDecimal[] getBigDecimalValuesForSVColumn(String column) {
    BigDecimal[] bigDecimalValues = getValues(FieldSpec.DataType.BIG_DECIMAL, column);
    if (markLoaded(FieldSpec.DataType.BIG_DECIMAL, column)) {
      if (bigDecimalValues == null) {
        bigDecimalValues = new BigDecimal[_length];
        putValues(FieldSpec.DataType.BIG_DECIMAL, column, bigDecimalValues);
      }
      _dataFetcher.fetchBigDecimalValues(column, _docIds, _length, bigDecimalValues);
    }
    return bigDecimalValues;
  }

  /**
   * Get the BigDecimal values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, BigDecimal[] buffer) {
    _dataFetcher.fetchBigDecimalValues(column, evaluator, _docIds, _length, buffer);
  }


  /**
   * Get the BigDecimal values for a single-valued column.
   *
   * @param column Column name
   * @return Array of BigDecimal values
   */
  public Vector[] getVectorValuesForSVColumn(String column) {
    Vector[] bigDecimalValues = getValues(FieldSpec.DataType.VECTOR, column);
    if (markLoaded(FieldSpec.DataType.VECTOR, column)) {
      if (bigDecimalValues == null) {
        bigDecimalValues = new Vector[_length];
        putValues(FieldSpec.DataType.VECTOR, column, bigDecimalValues);
      }
      _dataFetcher.fetchBigDecimalValues(column, _docIds, _length, bigDecimalValues);
    }
    return bigDecimalValues;
  }

  /**
   * Get the string values for a single-valued column.
   *
   * @param column Column name
   * @return Array of string values
   */
  public String[] getStringValuesForSVColumn(String column) {
    String[] stringValues = getValues(FieldSpec.DataType.STRING, column);
    if (markLoaded(FieldSpec.DataType.STRING, column)) {
      if (stringValues == null) {
        stringValues = new String[_length];
        putValues(FieldSpec.DataType.STRING, column, stringValues);
      }
      _dataFetcher.fetchStringValues(column, _docIds, _length, stringValues);
    }
    return stringValues;
  }

  /**
   * Get the string values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, String[] buffer) {
    _dataFetcher.fetchStringValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get byte[] values for the given single-valued column.
   *
   * @param column Column to read
   * @return byte[] for the column
   */
  public byte[][] getBytesValuesForSVColumn(String column) {
    byte[][] bytesValues = getValues(FieldSpec.DataType.BYTES, column);
    if (markLoaded(FieldSpec.DataType.BYTES, column)) {
      if (bytesValues == null) {
        bytesValues = new byte[_length][];
        putValues(FieldSpec.DataType.BYTES, column, bytesValues);
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
        dictIds = new int[_length][];
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
    int[][] intValues = getValues(FieldSpec.DataType.INT, column);
    if (markLoaded(FieldSpec.DataType.INT, column)) {
      if (intValues == null) {
        intValues = new int[_length][];
        putValues(FieldSpec.DataType.INT, column, intValues);
      }
      _dataFetcher.fetchIntValues(column, _docIds, _length, intValues);
    }
    return intValues;
  }

  /**
   * Get the int[][] values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, int[][] buffer) {
    _dataFetcher.fetchIntValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the long values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of long values
   */
  public long[][] getLongValuesForMVColumn(String column) {
    long[][] longValues = getValues(FieldSpec.DataType.LONG, column);
    if (markLoaded(FieldSpec.DataType.LONG, column)) {
      if (longValues == null) {
        longValues = new long[_length][];
        putValues(FieldSpec.DataType.LONG, column, longValues);
      }
      _dataFetcher.fetchLongValues(column, _docIds, _length, longValues);
    }
    return longValues;
  }

  /**
   * Get the long[][] values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, long[][] buffer) {
    _dataFetcher.fetchLongValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the float values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of float values
   */
  public float[][] getFloatValuesForMVColumn(String column) {
    float[][] floatValues = getValues(FieldSpec.DataType.FLOAT, column);
    if (markLoaded(FieldSpec.DataType.FLOAT, column)) {
      if (floatValues == null) {
        floatValues = new float[_length][];
        putValues(FieldSpec.DataType.FLOAT, column, floatValues);
      }
      _dataFetcher.fetchFloatValues(column, _docIds, _length, floatValues);
    }
    return floatValues;
  }

  /**
   * Get the float[][] values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, float[][] buffer) {
    _dataFetcher.fetchFloatValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the double values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of double values
   */
  public double[][] getDoubleValuesForMVColumn(String column) {
    double[][] doubleValues = getValues(FieldSpec.DataType.DOUBLE, column);
    if (markLoaded(FieldSpec.DataType.DOUBLE, column)) {
      if (doubleValues == null) {
        doubleValues = new double[_length][];
        putValues(FieldSpec.DataType.DOUBLE, column, doubleValues);
      }
      _dataFetcher.fetchDoubleValues(column, _docIds, _length, doubleValues);
    }
    return doubleValues;
  }

  /**
   * Get the double[][] values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, double[][] buffer) {
    _dataFetcher.fetchDoubleValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the string values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of string values
   */
  public String[][] getStringValuesForMVColumn(String column) {
    String[][] stringValues = getValues(FieldSpec.DataType.STRING, column);
    if (markLoaded(FieldSpec.DataType.STRING, column)) {
      if (stringValues == null) {
        stringValues = new String[_length][];
        putValues(FieldSpec.DataType.STRING, column, stringValues);
      }
      _dataFetcher.fetchStringValues(column, _docIds, _length, stringValues);
    }
    return stringValues;
  }

  /**
   * Get the String[][] values for a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param buffer values to fill
   */
  public void fillValues(String column, TransformEvaluator evaluator, String[][] buffer) {
    _dataFetcher.fetchStringValues(column, evaluator, _docIds, _length, buffer);
  }

  /**
   * Get the bytes values for a multi-valued column.
   *
   * @param column Column name
   * @return Array of bytes values
   */
  public byte[][][] getBytesValuesForMVColumn(String column) {
    byte[][][] bytesValues = getValues(FieldSpec.DataType.BYTES, column);
    if (markLoaded(FieldSpec.DataType.BYTES, column)) {
      if (bytesValues == null) {
        bytesValues = new byte[_length][][];
        putValues(FieldSpec.DataType.BYTES, column, bytesValues);
      }
      _dataFetcher.fetchBytesValues(column, _docIds, _length, bytesValues);
    }
    return bytesValues;
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
        numValues = new int[_length];
        _numValuesMap.put(column, numValues);
      }
      _dataFetcher.fetchNumValues(column, _docIds, _length, numValues);
    }
    return numValues;
  }

  private boolean markLoaded(FieldSpec.DataType dataType, String column) {
    return _columnValueLoaded.computeIfAbsent(dataType, k -> new HashSet<>()).add(column);
  }

  @SuppressWarnings("unchecked")
  private <T> T getValues(FieldSpec.DataType dataType, String column) {
    return (T) _valuesMap.computeIfAbsent(dataType, k -> new HashMap<>()).get(column);
  }

  private void putValues(FieldSpec.DataType dataType, String column, Object values) {
    _valuesMap.get(dataType).put(column, values);
  }
}
