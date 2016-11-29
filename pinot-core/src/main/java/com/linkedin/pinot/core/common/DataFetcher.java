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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

/**
 * DataFetcher is a higher level abstraction for data fetching. Given an index segment, DataFetcher can manage the
 * DataSource, Dictionary, BlockValSet and BlockValIterator for this segment, preventing redundant construction for
 * these instances. DataFetcher can be used by both selection, aggregation and group-by data fetching process, reducing
 * duplicate codes and garbage collection.
 */
public class DataFetcher {
  private static final BlockId BLOCK_ZERO = new BlockId(0);

  private final Map<String, BaseOperator> _columnToDataSourceMap;
  private final Map<String, Dictionary> _columnToDictionaryMap;
  private final Map<String, BlockValSet> _columnToBlockValSetMap;
  private final Map<String, BlockValIterator> _columnToBlockValIteratorMap;
  private final Map<String, BlockMetadata> _columnToBlockMetadataMap;

  /**
   * Constructor for DataFetcher.
   *
   * @param columnToDataSourceMap Map from column name to data source
   */
  public DataFetcher(Map<String, BaseOperator> columnToDataSourceMap) {
    _columnToDataSourceMap = columnToDataSourceMap;
    _columnToDictionaryMap = new HashMap<>();
    _columnToBlockValSetMap = new HashMap<>();
    _columnToBlockValIteratorMap = new HashMap<>();
    _columnToBlockMetadataMap = new HashMap<>();

    for (String column : columnToDataSourceMap.keySet()) {
      BaseOperator baseOperator = columnToDataSourceMap.get(column);
      Block block = baseOperator.nextBlock(BLOCK_ZERO);
      _columnToDictionaryMap.put(column, block.getMetadata().getDictionary());

      BlockValSet blockValSet = block.getBlockValueSet();
      _columnToBlockValSetMap.put(column, blockValSet);
      _columnToBlockValIteratorMap.put(column, blockValSet.iterator());
      _columnToBlockMetadataMap.put(column, block.getMetadata());
    }
  }

  /**
   * Given a column, fetch its data source.
   *
   * @param column column name.
   * @return data source associated with this column.
   */
  public BaseOperator getDataSourceForColumn(String column) {
    return _columnToDataSourceMap.get(column);
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
    BlockMultiValIterator iterator = (BlockMultiValIterator) getBlockValSetForColumn(column).iterator();
    for (int i = inStartPos; i < inStartPos + length; ++i) {
      iterator.skipTo(inDocIds[i]);
      int dictIdLength = iterator.nextIntVal(tempDictIdArray);
      outDictIdsArray[outStartPos++] = Arrays.copyOfRange(tempDictIdArray, 0, dictIdLength);
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
    return getDataSourceForColumn(column).nextBlock(BLOCK_ZERO).getMetadata().getMaxNumberOfMultiValues();
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
  public void fetchSingleIntValues(String column, int[] inDictIds, int inStartPos, int length, int[] outValues, int outStartPos) {
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
  public void fetchSingleLongValues(String column, int[] inDictIds, int inStartPos, int length, long[] outValues, int outStartPos) {
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
  public void fetchSingleFloatValues(String column, int[] inDictIds, int inStartPos, int length, float[] outValues, int outStartPos) {
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
  public void fetchSingleDoubleValues(String column, int[] inDictIds, int inStartPos, int length, double[] outValues, int outStartPos) {
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
  public void fetchSingleStringValues(String column, int[] inDictIds, int inStartPos, int length, String[] outValues, int outStartPos) {
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
  public void fetchSingleHashCodes(String column, int[] inDictIds, int inStartPos, int length, double[] outValues, int outStartPos) {
    Dictionary dictionary = getDictionaryForColumn(column);
    int inEndPos = inStartPos + length;
    for (int i = inStartPos; i < inEndPos; i++) {
      outValues[outStartPos++] = dictionary.get(inDictIds[i]).hashCode();
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
}
