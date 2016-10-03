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
package com.linkedin.pinot.core.operator.aggregation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;


/**
 * This class serves as a single/multi value column block level cache. Using this class can prevent fetching the same column
 * data multiple times. This class allocate resources on demand, and reuse them as much as possible to prevent garbage
 * collection.
 */
public class DataBlockCache {
  private final DataFetcher _dataFetcher;

  /** _columnXXLoaded must be cleared in initNewBlock */
  private final Set<String> _columnDictIdLoaded = new HashSet<>();
  private final Set<String> _columnValueLoaded = new HashSet<>();
  private final Set<String> _columnHashCodeLoaded = new HashSet<>();
  private final Set<String> _columnStringLoaded = new HashSet<>();

  /** _columnToXXsMap must be defined accordingly */
  private final Map<String, int[]> _columnToDictIdsMap = new HashMap<>();
  private final Map<String, double[]> _columnToValuesMap = new HashMap<>();
  private final Map<String, double[]> _columnToHashCodesMap = new HashMap<>();
  private final Map<String, String[]> _columnToStringsMap = new HashMap<>();

  private final Map<String, int[]> _columnToNumberOfEntriesMap = new HashMap<>();

  private final Map<String, int[][]> _columnToDictIdsArrayMap = new HashMap<>();
  private final Map<String, double[][]> _columnToValuesArrayMap = new HashMap<>();
  private final Map<String, double[][]> _columnToHashCodesArrayMap = new HashMap<>();
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
  private int[] getDictIdArrayForColumn(String column) {
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
   * Get an arrary of array representation for dictionary ids for a given column for the
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
    if  (!_columnToTempDictIdsMap.containsKey(column)) {
      int maxNumberOfEntries = _dataFetcher.getMaxNumberOfEntriesForColumn(column);
      int[] tempDictIdArray = new int[maxNumberOfEntries];
      _columnToTempDictIdsMap.put(column, tempDictIdArray);
    }
    return _columnToTempDictIdsMap.get(column);
  }
  /**
   * Get double value array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return value array associated with this column.
   */
  public double[] getDoubleValueArrayForColumn(String column) {
    double[] doubleValues = _columnToValuesMap.get(column);
    if (!_columnValueLoaded.contains(column)) {
      if (doubleValues == null) {
        doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToValuesMap.put(column, doubleValues);
      }
      int[] dictIds = getDictIdArrayForColumn(column);
      _dataFetcher.fetchSingleDoubleValues(column, dictIds, _startPos, _length, doubleValues, 0);
      _columnValueLoaded.add(column);
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
    double[][] doubleValueArrayArray = _columnToValuesArrayMap.get(column);
    if (!_columnValueLoaded.contains(column)) {
      if (doubleValueArrayArray == null) {
        doubleValueArrayArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToValuesArrayMap.put(column, doubleValueArrayArray);
      }
      int[][] dictIdsArray = getDictIdsArrayForColumn(column);
      for (int pos = 0; pos < _length; ++pos) {
        int[] dictIds = dictIdsArray[pos];
        double[] doubleValues = new double[dictIds.length];
        _dataFetcher.fetchSingleDoubleValues(column, dictIds, 0, dictIds.length, doubleValues, 0);
        doubleValueArrayArray[pos] = doubleValues;
      }
      _columnValueLoaded.add(column);
    }
    return doubleValueArrayArray;
  }

  /**
   * Get hash code array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return hash code array associated with this column.
   */
  public double[] getHashCodeArrayForColumn(String column) {
    double[] hashCodes = _columnToHashCodesMap.get(column);
    if (!_columnHashCodeLoaded.contains(column)) {
      if (hashCodes == null) {
        hashCodes = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
        _columnToHashCodesMap.put(column, hashCodes);
      }
      int[] dictIds = getDictIdArrayForColumn(column);
      _dataFetcher.fetchSingleHashCodes(column, dictIds, _startPos, _length, hashCodes, 0);
      _columnHashCodeLoaded.add(column);
    }
    return hashCodes;
  }

  /**
   * Get hash code array for a given column for the specific block initialized in the initNewBlock.
   *
   * @param column column name.
   * @return hash codes array associated with this column.
   */
  public double[][] getHashCodesArrayForColumn(String column) {
    double[][] hashCodesArray = _columnToHashCodesArrayMap.get(column);
    if (!_columnHashCodeLoaded.contains(column)) {
      if (hashCodesArray == null) {
        hashCodesArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
        _columnToHashCodesArrayMap.put(column, hashCodesArray);
      }
      int[][] dictIdsArray = getDictIdsArrayForColumn(column);
      for (int pos = 0; pos < _length; ++pos) {
        int[] dictIds = dictIdsArray[pos];
        double[] hashCodes = new double[dictIds.length];
        _dataFetcher.fetchSingleHashCodes(column, dictIds, 0, dictIds.length, hashCodes, 0);
        hashCodesArray[pos] = hashCodes;

      }
      _columnHashCodeLoaded.add(column);
    }
    return hashCodesArray;
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
      int[] dictIds = getDictIdArrayForColumn(column);
      _dataFetcher.fetchSingleStringValues(column, dictIds, _startPos, _length, stringValues, 0);
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
      int[][] dictIdsArray = getDictIdsArrayForColumn(column);
      for (int pos = 0; pos < _length; ++pos) {
        int[] dictIds = dictIdsArray[pos];
        String[] strings = new String[dictIds.length];
        _dataFetcher.fetchSingleStringValues(column, dictIds, 0, dictIds.length, strings, 0);
        stringsArray[pos] = strings;
      }
      _columnHashCodeLoaded.add(column);
    }
    return stringsArray;
  }

}
