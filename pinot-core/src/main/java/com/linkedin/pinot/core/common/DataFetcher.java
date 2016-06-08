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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.HashMap;
import java.util.Map;


/**
 * DataFetcher is a higher level abstraction for data fetching. Given an index segment, DataFetcher can manage the
 * DataSource, Dictionary, BlockValSet and BlockValIterator for this segment, preventing redundant construction for
 * these instances. DataFetcher can be used by both selection, aggregation and group-by data fetching process, reducing
 * duplicate codes and garbage collection.
 */
public class DataFetcher {
  private final IndexSegment _indexSegment;

  private final Map<String, DataSource> _columnToDataSourceMap = new HashMap<>();
  private final Map<String, Dictionary> _columnToDictionaryMap = new HashMap<>();
  private final Map<String, BlockValSet> _columnToBlockValSetMap = new HashMap<>();

  /**
   * Constructor for DataFetcher.
   *
   * @param indexSegment index segment.
   */
  public DataFetcher(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
  }

  /**
   * Given a column, fetch its data source.
   *
   * @param column column name.
   * @return data source associated with this column.
   */
  public DataSource getDataSourceForColumn(String column) {
    DataSource dataSource = _columnToDataSourceMap.get(column);
    if (dataSource == null) {
      dataSource = _indexSegment.getDataSource(column);
      _columnToDataSourceMap.put(column, dataSource);
    }
    return dataSource;
  }

  /**
   * Given a column, fetch its dictionary.
   *
   * @param column column name.
   * @return dictionary associated with this column.
   */
  public Dictionary getDictionaryForColumn(String column) {
    Dictionary dictionary = _columnToDictionaryMap.get(column);
    if (dictionary == null) {
      dictionary = getDataSourceForColumn(column).getDictionary();
      _columnToDictionaryMap.put(column, dictionary);
    }
    return dictionary;
  }

  /**
   * Given a column, fetch its block value set.
   *
   * @param column column name.
   * @return block value set associated with this column.
   */
  public BlockValSet getBlockValSetForColumn(String column) {
    BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
    if (blockValSet == null) {
      blockValSet = getDataSourceForColumn(column).getNextBlock().getBlockValueSet();
      _columnToBlockValSetMap.put(column, blockValSet);
    }
    return blockValSet;
  }

  /**
   * Fetch the dictionary Ids for a single value column.
   *
   * @param column column name.
   * @param inDocIds document Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outDictIds dictionary Id array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleDictIds(String column, int[] inDocIds, int inStartPos, int length, int[] outDictIds,
      int outStartPos) {
    BlockValSet blockValSet = getBlockValSetForColumn(column);
    blockValSet.readIntValues(inDocIds, inStartPos, length, outDictIds, outStartPos);
  }

  /**
   * Fetch the values for a single int value column.
   *
   * @param column column name.
   * @param inDictIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleIntValues(String column, int[] inDictIds, int inStartPos, int length, int[] outValues,
      int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    dictionary.readIntValues(inDictIds, inStartPos, length, outValues, outStartPos);
  }

  /**
   * Fetch the values for a single long value column.
   *
   * @param column column name.
   * @param inDictIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleLongValues(String column, int[] inDictIds, int inStartPos, int length, long[] outValues,
      int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    dictionary.readLongValues(inDictIds, inStartPos, length, outValues, outStartPos);
  }

  /**
   * Fetch the values for a single float value column.
   *
   * @param column column name.
   * @param inDictIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleFloatValues(String column, int[] inDictIds, int inStartPos, int length, float[] outValues,
      int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    dictionary.readFloatValues(inDictIds, inStartPos, length, outValues, outStartPos);
  }

  /**
   * Fetch the values for a single double value column.
   *
   * @param column column name.
   * @param inDictIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleDoubleValues(String column, int[] inDictIds, int inStartPos, int length, double[] outValues,
      int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    dictionary.readDoubleValues(inDictIds, inStartPos, length, outValues, outStartPos);
  }

  /**
   * Fetch the values for a single String value column.
   *
   * @param column column name.
   * @param inDictIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleStringValues(String column, int[] inDictIds, int inStartPos, int length, String[] outValues,
      int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    dictionary.readStringValues(inDictIds, inStartPos, length, outValues, outStartPos);
  }

  /**
   * Fetch the hash code values for a single value column.
   *
   * @param column column name.
   * @param inDictIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchSingleHashCodes(String column, int[] inDictIds, int inStartPos, int length, double[] outValues,
      int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = dictionary.get(inDictIds[i]).hashCode();
    }
  }
}
