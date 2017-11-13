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
import com.linkedin.pinot.core.common.DataBlockCache;
import com.linkedin.pinot.core.operator.MProjectionOperator;


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
   * so that it can be reused across multiple calls to {@link MProjectionOperator#nextBlock()}.
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

  @Override
  public int[] getIntValuesSV() {
    return _dataBlockCache.getIntValueArrayForColumn(_column);
  }

  @Override
  public int[][] getIntValuesMV() {
    return _dataBlockCache.getIntValuesArrayForColumn(_column);
  }

  @Override
  public long[] getLongValuesSV() {
    return _dataBlockCache.getLongValueArrayForColumn(_column);
  }

  @Override
  public long[][] getLongValuesMV() {
    return _dataBlockCache.getLongValuesArrayForColumn(_column);
  }

  @Override
  public float[] getFloatValuesSV() {
    return _dataBlockCache.getFloatValueArrayForColumn(_column);
  }

  @Override
  public float[][] getFloatValuesMV() {
    return _dataBlockCache.getFloatValuesArrayForColumn(_column);
  }

  /**
   * Returns an array of single values for the given doc Ids.
   * Caller chooses T based on data type.
   *
   * @return Array of single-values from dataBlockCache.
   */
  @Override
  public double[] getDoubleValuesSV() {
    return _dataBlockCache.getDoubleValueArrayForColumn(_column);
  }

  /**
   * Returns an array of multi-values for the given doc Ids.
   * Caller chooses T based on data type.
   *
   * @return Array of multi-values from dataBlockCache.
   */
  @Override
  public double[][] getDoubleValuesMV() {
    return _dataBlockCache.getDoubleValuesArrayForColumn(_column);
  }


  @Override
  public String[] getStringValuesSV() {
    return _dataBlockCache.getStringValueArrayForColumn(_column);
  }

  @Override
  public String[][] getStringValuesMV() {
    return _dataBlockCache.getStringValuesArrayForColumn(_column);
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

  @Override
  public int[] getNumberOfMVEntriesArray() {
    return _dataBlockCache.getNumberOfEntriesArrayForColumn(_column);
  }
}
