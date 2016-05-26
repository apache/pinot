/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * This class serves as a single value column block level cache. Using this class can prevent fetching the same column
 * data multiple times. This class allocate resources on demand, and reuse them as much as possible to prevent garbage
 * collection.
 */
public class SingleValueBlockCache {
  private final DataFetcher _dataFetcher;
  private final Set<String> _columnDictIdLoaded = new HashSet<>();
  private final Set<String> _columnValueLoaded = new HashSet<>();
  private final Set<String> _columnHashCodeLoaded = new HashSet<>();
  private final Map<String, int[]> _columnToDictIdsMap = new HashMap<>();
  private final Map<String, double[]> _columnToValuesMap = new HashMap<>();
  private final Map<String, double[]> _columnToHashCodesMap = new HashMap<>();

  private int[] _docIds;
  private int _startPos;
  private int _length;

  /**
   * Constructor for SingleValueBlockCache.
   *
   * @param dataFetcher data fetcher associated with the index segment.
   */
  public SingleValueBlockCache(DataFetcher dataFetcher) {
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
}
