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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.operator.BaseOperator;
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
  private final Map<String, Dictionary> _columnToDictionaryMap;
  private final Map<String, BlockValSet> _columnToBlockValSetMap;
  private final Map<String, BlockValIterator> _columnToBlockValIteratorMap;
  private final Map<String, BlockMetadata> _columnToBlockMetadataMap;

  // Map from MV column name to max number of entries for the column.
  private final Map<String, Integer> _columnToMaxNumMultiValuesMap;

  // Thread local (reusable) array for all dictionary ids in the block, of a single valued column
  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_IDS = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  };

  // Re-usable array to store MV dictionary id's for a given docId
  private static final ThreadLocal<int[]> THREAD_LOCAL_MV_DICT_IDS = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      // Size is known only at runtime, which is when the array is expanded.
      return new int[0];
    }
  };

  private int _reusableMVDictIdSize;

  /**
   * Constructor for DataFetcher.
   *
   * @param columnToDataSourceMap Map from column name to data source
   */
  public DataFetcher(Map<String, BaseOperator> columnToDataSourceMap) {
    _columnToDictionaryMap = new HashMap<>();
    _columnToBlockValSetMap = new HashMap<>();
    _columnToBlockValIteratorMap = new HashMap<>();
    _columnToBlockMetadataMap = new HashMap<>();
    _columnToMaxNumMultiValuesMap = new HashMap<>();

    _reusableMVDictIdSize = 0;
    for (String column : columnToDataSourceMap.keySet()) {
      BaseOperator dataSource = columnToDataSourceMap.get(column);
      Block dataSourceBlock = dataSource.nextBlock();
      BlockMetadata metadata = dataSourceBlock.getMetadata();
      _columnToDictionaryMap.put(column, metadata.getDictionary());

      BlockValSet blockValSet = dataSourceBlock.getBlockValueSet();
      _columnToBlockValSetMap.put(column, blockValSet);
      _columnToBlockValIteratorMap.put(column, blockValSet.iterator());
      _columnToBlockMetadataMap.put(column, metadata);

      int maxNumberOfMultiValues = metadata.getMaxNumberOfMultiValues();
      _columnToMaxNumMultiValuesMap.put(column, maxNumberOfMultiValues);
      _reusableMVDictIdSize = Math.max(_reusableMVDictIdSize, maxNumberOfMultiValues);
    }
  }

  /**
   * Given a column, fetch its dictionary.
   *
   * @param column column name.
   * @return dictionary associated with this column.
   */
  public Dictionary getDictionaryForColumn(String column) {
    return _columnToDictionaryMap.get(column);
  }

  /**
   * Given a column, fetch its block value set.
   *
   * @param column column name.
   * @return block value set associated with this column.
   */
  public BlockValSet getBlockValSetForColumn(String column) {
    return _columnToBlockValSetMap.get(column);
  }

  /**
   * Returns the BlockValIterator for the specified column.
   *
   * @param column Column for which to return the blockValIterator.
   * @return BlockValIterator for the column.
   */
  public BlockValIterator getBlockValIteratorForColumn(String column) {
    return _columnToBlockValIteratorMap.get(column);
  }

  public BlockMetadata getBlockMetadataFor(String column) {
    return _columnToBlockMetadataMap.get(column);
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
  public void fetchSingleDictIds(String column, int[] inDocIds, int inStartPos, int length, int[] outDictIds, int outStartPos) {
    BlockValSet blockValSet = getBlockValSetForColumn(column);
    blockValSet.getDictionaryIds(inDocIds, inStartPos, length, outDictIds, outStartPos);
  }

  /**
   * Fetch the dictionary Ids for a multi value column.
   *
   * @param column column name.
   * @param inDocIds document Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outDictIdsArray dictionary Id array array buffer.
   * @param outStartPos output start position.
   * @param tempDictIdArray temporary holding dictIds read from BlockMultiValIterator.
   *          Array size has to be >= max number of entries for this column.
   */
  public void fetchMultiValueDictIds(String column, int[] inDocIds, int inStartPos, int length, int[][] outDictIdsArray, int outStartPos,
      int[] tempDictIdArray) {
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValIteratorForColumn(column);
    for (int i = inStartPos; i < inStartPos + length; i++, outStartPos++) {
      iterator.skipTo(inDocIds[i]);
      int dictIdLength = iterator.nextIntVal(tempDictIdArray);
      outDictIdsArray[outStartPos] = Arrays.copyOfRange(tempDictIdArray, 0, dictIdLength);
    }
  }

  /**
   * For a given multi-value column, trying to get the max number of
   * entries per row.
   *
   * @param column Column for which to get the max number of multi-values.
   * @return max number of entries for a given column.
   */
  public int getMaxNumberOfEntriesForColumn(String column) {
    return _columnToMaxNumMultiValuesMap.get(column);
  }

  /**
   * Fetch the values for a single int value column.
   *
   * @param column column name.
   * @param inDocIds doc Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchIntValues(String column, int[] inDocIds, int inStartPos, int length, int[] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchSingleDictIds(column, inDocIds, inStartPos, length, dictIds, 0);
      dictionary.readIntValues(dictIds, 0, length, outValues, outStartPos);
    } else {
      BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
      blockValSet.getIntValues(inDocIds, inStartPos, length, outValues, outStartPos);
    }
  }

  /**
   * Fetch the int values for a multi-valued column.
   *
   * @param column column name.
   * @param inDocIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchIntValues(String column, int[] inDocIds, int inStartPos, int length, int[][] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValIteratorForColumn(column);

    int inEndPos = inStartPos + length;
    int[] reusableMVDictIds = getReusableMVDictIds(_reusableMVDictIdSize);
    for (int i = inStartPos; i < inEndPos; i++, outStartPos++) {
      iterator.skipTo(inDocIds[i]);
      int numValues = iterator.nextIntVal(reusableMVDictIds);
      outValues[outStartPos] = new int[numValues];
      dictionary.readIntValues(reusableMVDictIds, 0, numValues, outValues[outStartPos], 0);
    }
  }

  /**
   * Fetch the values for a single long value column.
   *
   * @param column column name.
   * @param inDocIds doc Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchLongValues(String column, int[] inDocIds, int inStartPos, int length, long[] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchSingleDictIds(column, inDocIds, inStartPos, length, dictIds, 0);
      dictionary.readLongValues(dictIds, 0, length, outValues, outStartPos);
    } else {
      BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
      blockValSet.getLongValues(inDocIds, inStartPos, length, outValues, outStartPos);
    }
  }

  /**
   * Fetch the long values for a multi-valued column.
   *
   * @param column column name.
   * @param inDocIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchLongValues(String column, int[] inDocIds, int inStartPos, int length, long[][] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValIteratorForColumn(column);

    int inEndPos = inStartPos + length;
    int[] reusableMVDictIds = getReusableMVDictIds(_reusableMVDictIdSize);
    for (int i = inStartPos; i < inEndPos; i++, outStartPos++) {
      iterator.skipTo(inDocIds[i]);
      int numValues = iterator.nextIntVal(reusableMVDictIds);
      outValues[outStartPos] = new long[numValues];
      dictionary.readLongValues(reusableMVDictIds, 0, numValues, outValues[outStartPos], 0);
    }
  }

  /**
   * Fetch the values for a single float value column.
   *
   * @param column column name.
   * @param inDocIds doc Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchFloatValues(String column, int[] inDocIds, int inStartPos, int length, float[] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchSingleDictIds(column, inDocIds, inStartPos, length, dictIds, 0);
      dictionary.readFloatValues(dictIds, 0, length, outValues, outStartPos);
    } else {
      BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
      blockValSet.getFloatValues(inDocIds, inStartPos, length, outValues, outStartPos);
    }
  }

  /**
   * Fetch the float values for a multi-valued column.
   *
   * @param column column name.
   * @param inDocIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchFloatValues(String column, int[] inDocIds, int inStartPos, int length, float[][] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValIteratorForColumn(column);

    int inEndPos = inStartPos + length;
    int[] reusableMVDictIds = getReusableMVDictIds(_reusableMVDictIdSize);
    for (int i = inStartPos; i < inEndPos; i++, outStartPos++) {
      iterator.skipTo(inDocIds[i]);
      int numValues = iterator.nextIntVal(reusableMVDictIds);
      outValues[outStartPos] = new float[numValues];
      dictionary.readFloatValues(reusableMVDictIds, 0, numValues, outValues[outStartPos], 0);
    }
  }

  /**
   * Fetch the values for a single double value column.
   *
   * @param column column name.
   * @param inDocIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchDoubleValues(String column, int[] inDocIds, int inStartPos, int length, double[] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchSingleDictIds(column, inDocIds, inStartPos, length, dictIds, 0);
      dictionary.readDoubleValues(dictIds, 0, length, outValues, outStartPos);
    } else {
      BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
      blockValSet.getDoubleValues(inDocIds, inStartPos, length, outValues, outStartPos);
    }
  }

  /**
   * Fetch the double values for a multi-valued column.
   *
   * @param column column name.
   * @param inDocIds dictionary Id array.
   * @param inStartPos input start position.
   * @param length input length.
   * @param outValues value array buffer.
   * @param outStartPos output start position.
   */
  public void fetchDoubleValues(String column, int[] inDocIds, int inStartPos, int length, double[][] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValIteratorForColumn(column);

    int inEndPos = inStartPos + length;
    int[] reusableMVDictIds = getReusableMVDictIds(_reusableMVDictIdSize);
    for (int i = inStartPos; i < inEndPos; i++, outStartPos++) {
      iterator.skipTo(inDocIds[i]);
      int numValues = iterator.nextIntVal(reusableMVDictIds);
      outValues[outStartPos] = new double[numValues];
      dictionary.readDoubleValues(reusableMVDictIds, 0, numValues, outValues[outStartPos], 0);
    }
  }

  /**
   *
   * @param column Column for which to fetch the values
   * @param inDocIds Array of docIds for which to fetch the values
   * @param outValues Array of strings where output will be written
   * @param length Length of input docIds
   */
  public void fetchStringValues(String column, int[] inDocIds, int inStartPos, int length, String[] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    if (dictionary != null) {
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      fetchSingleDictIds(column, inDocIds, inStartPos, length, dictIds, 0);
      dictionary.readStringValues(dictIds, 0, length, outValues, outStartPos);
    } else {
      BlockValSet blockValSet = _columnToBlockValSetMap.get(column);
      blockValSet.getStringValues(inDocIds, inStartPos, length, outValues, outStartPos);
    }
  }

  /**
   *
   * @param column Column for which to fetch the values
   * @param inDocIds Array of docIds for which to fetch the values
   * @param outValues Array of strings where output will be written
   * @param length Length of input docIds
   */
  public void fetchStringValues(String column, int[] inDocIds, int inStartPos, int length, String[][] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValIteratorForColumn(column);

    int inEndPos = inStartPos + length;
    int[] reusableMVDictIds = getReusableMVDictIds(_reusableMVDictIdSize);
    for (int i = inStartPos; i < inEndPos; i++, outStartPos++) {
      iterator.skipTo(inDocIds[i]);
      int numValues = iterator.nextIntVal(reusableMVDictIds);
      outValues[outStartPos] = new String[numValues];
      dictionary.readStringValues(reusableMVDictIds, 0, numValues, outValues[outStartPos], 0);
    }
  }

  /**
   * Returns the data type for the specified column.
   *
   * @param column Name of column for which to return the data type.
   * @return Data type of the column.
   */
  public FieldSpec.DataType getDataType(String column) {
    BlockMetadata blockMetadata = _columnToBlockMetadataMap.get(column);
    Preconditions.checkNotNull(blockMetadata, "Invalid column " + column + " specified in DataFetcher.");
    return blockMetadata.getDataType();
  }

  /**
   * Helper method that returns ThreadLocal reusable int array for MV dictionary ids.
   * If desired size is larger than existing thread local storage, the latter is expanded.
   *
   * @param size Desired size.
   * @return Thread local int array of at least desired size.
   */
  private int[] getReusableMVDictIds(int size) {
    // If current size is not large enough, expand to new size.
    int[] reusableMVDictIds = THREAD_LOCAL_MV_DICT_IDS.get();

    if (reusableMVDictIds.length < size) {
      reusableMVDictIds = new int[size];
      THREAD_LOCAL_MV_DICT_IDS.set(reusableMVDictIds);
    }
    return reusableMVDictIds;
  }
}
