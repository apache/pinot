/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.operator.docvalsets.SingleValueSet;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * DataFetcher is a higher level abstraction for data fetching. Given an index segment, DataFetcher can manage the
 * DataSource, Dictionary, BlockValSet and BlockValIterator for this segment, preventing redundant construction for
 * these instances. DataFetcher can be used by both selection, aggregation and group-by data fetching process, reducing
 * duplicate codes and garbage collection.
 */
public class DataFetcher {
  // Thread local (reusable) buffer for single-valued column dictionary Ids
  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_IDS = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  };

  private final Map<String, Dictionary> _dictionaryMap;
  // For single-valued column
  private final Map<String, SingleValueSet> _singleValueSetMap;
  // For multi-valued column
  private final Map<String, BlockMultiValIterator> _blockMultiValIteratorMap;
  private final int[] _reusableMVDictIds;

  /**
   * Constructor for DataFetcher.
   *
   * @param dataSourceMap Map from column to data source
   */
  public DataFetcher(Map<String, DataSource> dataSourceMap) {
    int numColumns = dataSourceMap.size();
    _dictionaryMap = new HashMap<>(numColumns);
    _singleValueSetMap = new HashMap<>(numColumns);
    _blockMultiValIteratorMap = new HashMap<>(numColumns);

    int maxNumMultiValues = 0;
    for (Map.Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
      String column = entry.getKey();
      DataSource dataSource = entry.getValue();
      _dictionaryMap.put(column, dataSource.getDictionary());
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      BlockValSet blockValueSet = dataSource.nextBlock().getBlockValueSet();
      if (dataSourceMetadata.isSingleValue()) {
        _singleValueSetMap.put(column, (SingleValueSet) blockValueSet);
      } else {
        _blockMultiValIteratorMap.put(column, (BlockMultiValIterator) blockValueSet.iterator());
        maxNumMultiValues = Math.max(maxNumMultiValues, dataSourceMetadata.getMaxNumMultiValues());
      }
    }

    _reusableMVDictIds = new int[maxNumMultiValues];
  }

  /**
   * SINGLE-VALUED COLUMN API
   */

  /**
   * Fetch the dictionary Ids for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outDictIds Buffer for output
   */
  public void fetchDictIds(String column, int[] inDocIds, int length, int[] outDictIds) {
    _singleValueSetMap.get(column).getDictionaryIds(inDocIds, 0, length, outDictIds, 0);
  }

  /**
   * Fetch the int values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchIntValues(String column, int[] inDocIds, int length, int[] outValues) {
    Dictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchDictIds(column, inDocIds, length, dictIds);
      dictionary.readIntValues(dictIds, 0, length, outValues, 0);
    } else {
      _singleValueSetMap.get(column).getIntValues(inDocIds, 0, length, outValues, 0);
    }
  }

  /**
   * Fetch the long values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchLongValues(String column, int[] inDocIds, int length, long[] outValues) {
    Dictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchDictIds(column, inDocIds, length, dictIds);
      dictionary.readLongValues(dictIds, 0, length, outValues, 0);
    } else {
      _singleValueSetMap.get(column).getLongValues(inDocIds, 0, length, outValues, 0);
    }
  }

  /**
   * Fetch the float values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchFloatValues(String column, int[] inDocIds, int length, float[] outValues) {
    Dictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchDictIds(column, inDocIds, length, dictIds);
      dictionary.readFloatValues(dictIds, 0, length, outValues, 0);
    } else {
      _singleValueSetMap.get(column).getFloatValues(inDocIds, 0, length, outValues, 0);
    }
  }

  /**
   * Fetch the double values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchDoubleValues(String column, int[] inDocIds, int length, double[] outValues) {
    Dictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchDictIds(column, inDocIds, length, dictIds);
      dictionary.readDoubleValues(dictIds, 0, length, outValues, 0);
    } else {
      _singleValueSetMap.get(column).getDoubleValues(inDocIds, 0, length, outValues, 0);
    }
  }

  /**
   * Fetch the string values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchStringValues(String column, int[] inDocIds, int length, String[] outValues) {
    Dictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchDictIds(column, inDocIds, length, dictIds);
      dictionary.readStringValues(dictIds, 0, length, outValues, 0);
    } else {
      _singleValueSetMap.get(column).getStringValues(inDocIds, 0, length, outValues, 0);
    }
  }

  /**
   * Fetch byte[] values for a single-valued column.
   *
   * @param column Column to read
   * @param inDocIds Input document id's buffer
   * @param length Number of input document id'
   * @param outValues Buffer for output
   */
  public void fetchBytesValues(String column, int[] inDocIds, int length, byte[][] outValues) {
    Dictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchDictIds(column, inDocIds, length, dictIds);
      dictionary.readBytesValues(dictIds, 0, length, outValues, 0);
    } else {
      _singleValueSetMap.get(column).getBytesValues(inDocIds, 0, length, outValues, 0);
    }
  }

  /**
   * MULTI-VALUED COLUMN API
   */

  /**
   * Fetch the dictionary Ids for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outDictIds Buffer for output
   */
  public void fetchDictIds(String column, int[] inDocIds, int length, int[][] outDictIds) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      int numMultiValues = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
      outDictIds[i] = Arrays.copyOfRange(_reusableMVDictIds, 0, numMultiValues);
    }
  }

  /**
   * Fetch the int values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchIntValues(String column, int[] inDocIds, int length, int[][] outValues) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      int numMultiValues = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
      outValues[i] = new int[numMultiValues];
      _dictionaryMap.get(column).readIntValues(_reusableMVDictIds, 0, numMultiValues, outValues[i], 0);
    }
  }

  /**
   * Fetch the long values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchLongValues(String column, int[] inDocIds, int length, long[][] outValues) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      int numMultiValues = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
      outValues[i] = new long[numMultiValues];
      _dictionaryMap.get(column).readLongValues(_reusableMVDictIds, 0, numMultiValues, outValues[i], 0);
    }
  }

  /**
   * Fetch the float values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchFloatValues(String column, int[] inDocIds, int length, float[][] outValues) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      int numMultiValues = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
      outValues[i] = new float[numMultiValues];
      _dictionaryMap.get(column).readFloatValues(_reusableMVDictIds, 0, numMultiValues, outValues[i], 0);
    }
  }

  /**
   * Fetch the double values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchDoubleValues(String column, int[] inDocIds, int length, double[][] outValues) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      int numMultiValues = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
      outValues[i] = new double[numMultiValues];
      _dictionaryMap.get(column).readDoubleValues(_reusableMVDictIds, 0, numMultiValues, outValues[i], 0);
    }
  }

  /**
   * Fetch the string values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchStringValues(String column, int[] inDocIds, int length, String[][] outValues) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      int numMultiValues = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
      outValues[i] = new String[numMultiValues];
      _dictionaryMap.get(column).readStringValues(_reusableMVDictIds, 0, numMultiValues, outValues[i], 0);
    }
  }

  /**
   * Fetch the number of values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outNumValues Buffer for output
   */
  public void fetchNumValues(String column, int[] inDocIds, int length, int[] outNumValues) {
    BlockMultiValIterator blockMultiValIterator = _blockMultiValIteratorMap.get(column);
    for (int i = 0; i < length; i++) {
      blockMultiValIterator.skipTo(inDocIds[i]);
      outNumValues[i] = blockMultiValIterator.nextIntVal(_reusableMVDictIds);
    }
  }
}
