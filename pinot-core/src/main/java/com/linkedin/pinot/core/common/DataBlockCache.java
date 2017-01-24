/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * This class serves as a single/multi value column block level cache. Using this class can prevent fetching the same column
 * data multiple times. This class allocate resources on demand, and reuse them as much as possible to prevent garbage
 * collection.
 */
@SuppressWarnings("Duplicates")
public class DataBlockCache {
  private final DataFetcher _dataFetcher;

  /** _columnXXLoaded must be cleared in initNewBlock */
  private final Set<String> _columnDictIdLoaded = new HashSet<>();
  private final Set<String> _columnValueLoaded = new HashSet<>();
  private final Set<String> _columnHashCodeLoaded = new HashSet<>();
  private final Set<String> _columnStringLoaded = new HashSet<>();

  /** _columnToXXsMap must be defined accordingly */
  private final Map<String, int[]> _columnToDictIdsMap = new HashMap<>();
  private final Map<String, Object> _columnToValuesMap = new HashMap<>();
  private final Map<String, String[]> _columnToStringsMap = new HashMap<>();

  private final Map<String, int[]> _columnToNumberOfEntriesMap = new HashMap<>();

  private final Map<String, int[][]> _columnToDictIdsArrayMap = new HashMap<>();
  private final Map<String, Object> _columnToValuesArrayMap = new HashMap<>();
  private final Map<String, String[][]> _columnToStringsArrayMap = new HashMap<>();

  private final Map<String, int[]> _columnToTempDictIdsMap = new HashMap<>();

  private int[] _docIds;
  private int _startPos;
  private int _length;

  /**
   * Constructor for SingleValueBlockCache.
   *
   * @param dataFetcher data fetcher associated with the index segment.
   */
  public DataBlockCache(DataFetcher dataFetcher) {
    _dataFetcher = dataFetcher;
  }

  /**
   * Init the block cache with block doc id array, start index and block length. This method should be called before
   * fetching data for any specific block.
   *
   * @param docIds doc id array.
   * @param startPos start position.
   * @param length length.
   */
  public void initNewBlock(int[] docIds, int startPos, int length) {
    _columnDictIdLoaded.clear();
    _columnValueLoaded.clear();
    _columnHashCodeLoaded.clear();
    _columnStringLoaded.clear();

    _docIds = docIds;
    _startPos = startPos;
    _length = length;
  }

  /**
   * Get dictionary id array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return dictionary id array associated with this column.
   */
  public int[] getDictIdArrayForColumn(String column) {
    int[] dictIds = _columnToDictIdsMap.get(column);
    if (!_columnDictIdLoaded.contains(column)) {
      if (dictIds == null) {
        dictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToDictIdsMap.put(column, dictIds);
      }
      _dataFetcher.fetchSingleDictIds(column, _docIds, _startPos, _length, dictIds, 0);
      _columnDictIdLoaded.add(column);
    }
    return dictIds;
  }

  /**
   * Get an array of array representation for dictionary ids for a given column for the
   * specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return dictionary ids array associated with this column.
   */
  private int[][] getDictIdsArrayForColumn(String column) {
    int[][] dictIdsArray = _columnToDictIdsArrayMap.get(column);
    if (!_columnDictIdLoaded.contains(column)) {
      if (dictIdsArray == null) {
        dictIdsArray = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToDictIdsArrayMap.put(column, dictIdsArray);
      }
      _dataFetcher.fetchMultiValueDictIds(column, _docIds, _startPos, _length, dictIdsArray, 0,
          getTempDictIdArrayForColumn(column));
      _columnDictIdLoaded.add(column);
    }
    return dictIdsArray;
  }

  private int[] getTempDictIdArrayForColumn(String column) {
    if (!_columnToTempDictIdsMap.containsKey(column)) {
      int maxNumberOfEntries = _dataFetcher.getMaxNumberOfEntriesForColumn(column);
      int[] tempDictIdArray = new int[maxNumberOfEntries];
      _columnToTempDictIdsMap.put(column, tempDictIdArray);
    }
    return _columnToTempDictIdsMap.get(column);
  }

  /**
   * Get int value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public int[] getIntValueArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.INT);
    int[] intValues = (int []) _columnToValuesMap.get(key);
    if (!_columnValueLoaded.contains(key)) {
      if (intValues == null) {
        intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToValuesMap.put(key, intValues);
      }
      _dataFetcher.fetchIntValues(column, _docIds, _startPos, _length, intValues, 0);
      _columnValueLoaded.add(key);
    }
    return intValues;
  }

  /**
   * Get double value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return int values array associated with this column.
   */
  public int[][] getIntValuesArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.INT_ARRAY);
    int[][] intValues = (int[][]) _columnToValuesArrayMap.get(key);

    if (!_columnValueLoaded.contains(key)) {
      if (intValues == null) {
        intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToValuesArrayMap.put(key, intValues);
      }

      _dataFetcher.fetchIntValues(column, _docIds, _startPos, _length, intValues, 0);
      _columnValueLoaded.add(key);
    }
    return intValues;
  }

  /**
   * Get long value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public long[] getLongValueArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.LONG);
    long[] longValues = (long []) _columnToValuesMap.get(key);
    if (!_columnValueLoaded.contains(key)) {
      if (longValues == null) {
        longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToValuesMap.put(key, longValues);
      }
      _dataFetcher.fetchLongValues(column, _docIds, _startPos, _length, longValues, 0);
      _columnValueLoaded.add(key);
    }
    return longValues;
  }

  /**
   * Get long value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return long values array associated with this column.
   */
  public long[][] getLongValuesArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.LONG_ARRAY);
    long[][] longValues = (long[][]) _columnToValuesArrayMap.get(key);

    if (!_columnValueLoaded.contains(key)) {
      if (longValues == null) {
        longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToValuesArrayMap.put(key, longValues);
      }

      _dataFetcher.fetchLongValues(column, _docIds, _startPos, _length, longValues, 0);
      _columnValueLoaded.add(key);
    }
    return longValues;
  }

  /**
   * Get long value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public float[] getFloatValueArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.FLOAT);
    float[] floatValues = (float []) _columnToValuesMap.get(key);
    if (!_columnValueLoaded.contains(key)) {
      if (floatValues == null) {
        floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToValuesMap.put(key, floatValues);
      }
      _dataFetcher.fetchFloatValues(column, _docIds, _startPos, _length, floatValues, 0);
      _columnValueLoaded.add(key);
    }
    return floatValues;
  }

  /**
   * Get long value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return long values array associated with this column.
   */
  public float[][] getFloatValuesArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.FLOAT_ARRAY);
    float[][] floatValues = (float[][]) _columnToValuesArrayMap.get(key);

    if (!_columnValueLoaded.contains(key)) {
      if (floatValues == null) {
        floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToValuesArrayMap.put(key, floatValues);
      }

      _dataFetcher.fetchFloatValues(column, _docIds, _startPos, _length, floatValues, 0);
      _columnValueLoaded.add(key);
    }
    return floatValues;
  }

  /**
   * Get double value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public double[] getDoubleValueArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.DOUBLE);
    double[] doubleValues = (double []) _columnToValuesMap.get(key);
    if (!_columnValueLoaded.contains(key)) {
      if (doubleValues == null) {
        doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToValuesMap.put(key, doubleValues);
      }
      _dataFetcher.fetchDoubleValues(column, _docIds, _startPos, _length, doubleValues, 0);
      _columnValueLoaded.add(key);
    }
    return doubleValues;
  }

  /**
   * Get double value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return double values array associated with this column.
   */
  public double[][] getDoubleValuesArrayForColumn(String column) {
    String key = getKeyForColumnAndType(column, FieldSpec.DataType.DOUBLE_ARRAY);
    double[][] doubleValuesArray = (double[][]) _columnToValuesArrayMap.get(key);

    if (!_columnValueLoaded.contains(key)) {
      if (doubleValuesArray == null) {
        doubleValuesArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToValuesArrayMap.put(key, doubleValuesArray);
      }

      _dataFetcher.fetchDoubleValues(column, _docIds, _startPos, _length, doubleValuesArray, 0);
      _columnValueLoaded.add(key);
    }
    return doubleValuesArray;
  }

  /**
   * Get hash code array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return hash codes array associated with this column.
   */
  public int[] getNumberOfEntriesArrayForColumn(String column) {
    int[] numberOfEntriesArray = _columnToNumberOfEntriesMap.get(column);
    if (!_columnHashCodeLoaded.contains(column)) {
      if (numberOfEntriesArray == null) {
        numberOfEntriesArray = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToNumberOfEntriesMap.put(column, numberOfEntriesArray);
      }
      int[][] dictIdsArray = getDictIdsArrayForColumn(column);
      for (int pos = 0; pos < _length; ++pos) {
        numberOfEntriesArray[pos] = dictIdsArray[pos].length;
      }
      _columnHashCodeLoaded.add(column);
    }
    return numberOfEntriesArray;
  }

  /**
   * Get string value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public String[] getStringValueArrayForColumn(String column) {
    String[] stringValues = _columnToStringsMap.get(column);
    if (!_columnStringLoaded.contains(column)) {
      if (stringValues == null) {
        stringValues = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToStringsMap.put(column, stringValues);
      }
      _dataFetcher.fetchStringValues(column, _docIds, _startPos, _length, stringValues, 0);
      _columnStringLoaded.add(column);
    }
    return stringValues;
  }

  /**
   * Get string value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return string values array associated with this column.
   */
  public String[][] getStringValuesArrayForColumn(String column) {
    String[][] stringsArray = _columnToStringsArrayMap.get(column);
    if (!_columnHashCodeLoaded.contains(column)) {
      if (stringsArray == null) {
        stringsArray = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToStringsArrayMap.put(column, stringsArray);
      }

      _dataFetcher.fetchStringValues(column, _docIds, _startPos, _length, stringsArray, 0);
      _columnHashCodeLoaded.add(column);
    }
    return stringsArray;
  }

  /**
   * Returns the data type of the specified column.
   *
   * @param column Column for which to return the data type.
   * @return Data type of the column.
   */
  public FieldSpec.DataType getDataType(String column) {
    return _dataFetcher.getDataType(column);
  }

  /**
   * Returns the block metadata for the given column.
   *
   * @param column Column for which to get the metadata
   * @return Metadata for the column
   */
  public BlockMetadata getMetadataFor(String column) {
    return _dataFetcher.getBlockMetadataFor(column);
  }

  /**
   * Returns the data fetcher
   *
   * @return Data fetcher
   */
  public DataFetcher getDataFetcher() {
    return _dataFetcher;
  }

  /**
   * Helper method that generates a key for {@link #_columnToValuesMap} using column name and data type
   * to be fetched for the column.
   *
   * @param column Column Name
   * @param dataType Data Type
   * @return Key of column name and data type
   */
  private String getKeyForColumnAndType(String column, FieldSpec.DataType dataType) {
    StringBuilder builder = new StringBuilder(column);
    builder.append("_");
    builder.append(dataType);
    return builder.toString();
  }
}
