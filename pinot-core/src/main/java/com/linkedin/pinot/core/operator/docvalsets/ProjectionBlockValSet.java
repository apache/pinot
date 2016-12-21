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
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.DataBlockCache;


/**
 * This class represents the BlockValSet for a projection block.
 * It provides api's to access data for a specified projection column.
 * It uses {@link DataBlockCache} to cache the projection data.
 */
public class ProjectionBlockValSet extends BaseBlockValSet {

  private DataBlockCache _dataBlockCache;
  private final BlockValIterator _blockValIterator;
  private final FieldSpec.DataType _columnDataType;
  private String _column;

  /**


   * Constructor for the class.
   * The dataBlockCache argument is initialized in {@link com.linkedin.pinot.core.operator.MProjectionOperator},
   * so that it can be reused across multiple calls to {@link MProjectionOperator#getNextBlock()}.
   *
   * @param dataBlockCache data block cache
   * @param column Projection column.
   */
  public ProjectionBlockValSet(DataBlockCache dataBlockCache, String column) {
    _dataBlockCache = dataBlockCache;
    _column = column;
    _columnDataType = _dataBlockCache.getDataType(column);
    _blockValIterator = _dataBlockCache.getDataFetcher().getBlockValIteratorForColumn(column);
  }

  /**
   * Returns an array of single values for the given doc Ids.
   * Caller chooses T based on data type.
   *
   * @param <T> Return type.
   * @return Array of single-values from dataBlockCache.
   */
  @Override
  public <T> T getSingleValues() {
    return getValues(false);
  }

  /**
   * Returns an array of multi-values for the given doc Ids.
   * Caller chooses T based on data type.
   *
   * @param <T> Return type.
   * @return Array of multi-values from dataBlockCache.
   */
  @Override
  public <T> T getMultiValues() {
    return getValues(true);
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _columnDataType;
  }

  // TODO: Remove arguments from interface, as projection block will have the information.
  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds,
      int outStartPos) {
    getDictionaryIds();
  }

  @Override
  public int[] getDictionaryIds() {
    return _dataBlockCache.getDictIdArrayForColumn(_column);
  }

  @Override
  public int getDictionaryIdsForDocId(int docId, int[] outputDictIds) {
    BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _blockValIterator;
    blockValIterator.skipTo(docId);
    return blockValIterator.nextIntVal(outputDictIds);
  }

  /**
   * Returns array containing hash codes for values of the single-valued projection column, for the filtered docIds.
   * @return Array of hash codes for values of the projection column
   */
  public int[] getSVHashCodeArray() {
    return _dataBlockCache.getHashCodeArrayForColumn(_column);
  }

  /**
   * Returns array containing hash codes for values of the multi-valued projection column, for the filtered docIds.
   * @return Array of hash codes for values of the projection column
   */
  public int[][] getMVHashCodeArray() {
    return _dataBlockCache.getHashCodesArrayForColumn(_column);
  }

  /**
   * Returns an array containing number of MV entries for the filtered docIds of the projection column.
   * @return Array of number of MV entries
   */
  public int[] getNumberOfMVEntriesArray() {
    return _dataBlockCache.getNumberOfEntriesArrayForColumn(_column);
  }

  /**
   * Helper method to get SV/MV values for the projection column.
   *
   * @param multiValued True for multi-valued columns, False for single-valued columns.
   * @param <T> Return data type
   * @return Values for the projection column.
   */
  @SuppressWarnings("unchecked")
  private <T> T getValues(boolean multiValued) {
    switch (_columnDataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return (T) ((multiValued) ? _dataBlockCache.getDoubleValuesArrayForColumn(_column)
            : _dataBlockCache.getDoubleValueArrayForColumn(_column));

      case STRING:
        return (T) ((multiValued) ? _dataBlockCache.getStringValuesArrayForColumn(_column)
            : _dataBlockCache.getStringValueArrayForColumn(_column));

      case OBJECT:
        return (T) ((multiValued) ? _dataBlockCache.getHashCodesArrayForColumn(_column)
            : _dataBlockCache.getHashCodeArrayForColumn(_column));

      default:
        throw new RuntimeException("Data type " + _columnDataType + " not supported in ProjectionBlockValSet.");
    }
  }
}
