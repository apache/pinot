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
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.common.DataBlockCache;
import com.linkedin.pinot.core.operator.ProjectionOperator;


/**
 * This class represents the BlockValSet for a projection block.
 * It provides api's to access data for a specified projection column.
 * It uses {@link DataBlockCache} to cache the projection data.
 */
public class ProjectionBlockValSet extends BaseBlockValSet {
  private final DataBlockCache _dataBlockCache;
  private final String _column;
  private final FieldSpec.DataType _dataType;

  /**
   * Constructor for the class.
   * The dataBlockCache argument is initialized in {@link ProjectionOperator},
   * so that it can be reused across multiple calls to {@link ProjectionOperator#nextBlock()}.
   *
   * @param dataBlockCache data block cache
   * @param column Projection column.
   */
  public ProjectionBlockValSet(DataBlockCache dataBlockCache, String column, FieldSpec.DataType dataType) {
    _dataBlockCache = dataBlockCache;
    _column = column;
    _dataType = dataType;
  }

  @Override
  public int[] getIntValuesSV() {
    return _dataBlockCache.getIntValuesForSVColumn(_column);
  }

  @Override
  public int[][] getIntValuesMV() {
    return _dataBlockCache.getIntValuesForMVColumn(_column);
  }

  @Override
  public long[] getLongValuesSV() {
    return _dataBlockCache.getLongValuesForSVColumn(_column);
  }

  @Override
  public long[][] getLongValuesMV() {
    return _dataBlockCache.getLongValuesForMVColumn(_column);
  }

  @Override
  public float[] getFloatValuesSV() {
    return _dataBlockCache.getFloatValuesForSVColumn(_column);
  }

  @Override
  public float[][] getFloatValuesMV() {
    return _dataBlockCache.getFloatValuesForMVColumn(_column);
  }

  /**
   * Returns an array of single values for the given doc Ids.
   * Caller chooses T based on data type.
   *
   * @return Array of single-values from dataBlockCache.
   */
  @Override
  public double[] getDoubleValuesSV() {
    return _dataBlockCache.getDoubleValuesForSVColumn(_column);
  }

  /**
   * Returns an array of multi-values for the given doc Ids.
   * Caller chooses T based on data type.
   *
   * @return Array of multi-values from dataBlockCache.
   */
  @Override
  public double[][] getDoubleValuesMV() {
    return _dataBlockCache.getDoubleValuesForMVColumn(_column);
  }

  @Override
  public String[] getStringValuesSV() {
    return _dataBlockCache.getStringValuesForSVColumn(_column);
  }

  @Override
  public byte[][] getBytesValuesSV() {
    return _dataBlockCache.getBytesValuesForSVColumn(_column);
  }

  @Override
  public String[][] getStringValuesMV() {
    return _dataBlockCache.getStringValuesForMVColumn(_column);
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    return _dataBlockCache.getDictIdsForSVColumn(_column);
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    return _dataBlockCache.getDictIdsForMVColumn(_column);
  }

  @Override
  public int[] getNumMVEntries() {
    return _dataBlockCache.getNumValuesForMVColumn(_column);
  }
}
