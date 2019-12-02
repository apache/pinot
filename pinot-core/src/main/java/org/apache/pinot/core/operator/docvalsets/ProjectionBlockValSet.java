/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.docvalsets;

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.core.common.BaseBlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.ProjectionOperator;


/**
 * This class represents the BlockValSet for a projection block.
 * It provides api's to access data for a specified projection column.
 * It uses {@link DataBlockCache} to cache the projection data.
 */
public class ProjectionBlockValSet extends BaseBlockValSet {
  private final DataBlockCache _dataBlockCache;
  private final String _column;
  private final DataType _dataType;
  private final boolean _singleValue;

  /**
   * Constructor for the class.
   * The dataBlockCache argument is initialized in {@link ProjectionOperator},
   * so that it can be reused across multiple calls to {@link ProjectionOperator#nextBlock()}.
   *
   * @param dataBlockCache data block cache
   * @param column Projection column.
   */
  public ProjectionBlockValSet(DataBlockCache dataBlockCache, String column, DataType dataType, boolean singleValue) {
    _dataBlockCache = dataBlockCache;
    _column = column;
    _dataType = dataType;
    _singleValue = singleValue;
  }

  @Override
  public DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    return _singleValue;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    return _dataBlockCache.getDictIdsForSVColumn(_column);
  }

  @Override
  public int[] getIntValuesSV() {
    return _dataBlockCache.getIntValuesForSVColumn(_column);
  }

  @Override
  public long[] getLongValuesSV() {
    return _dataBlockCache.getLongValuesForSVColumn(_column);
  }

  @Override
  public float[] getFloatValuesSV() {
    return _dataBlockCache.getFloatValuesForSVColumn(_column);
  }

  @Override
  public double[] getDoubleValuesSV() {
    return _dataBlockCache.getDoubleValuesForSVColumn(_column);
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
  public int[][] getDictionaryIdsMV() {
    return _dataBlockCache.getDictIdsForMVColumn(_column);
  }

  @Override
  public int[][] getIntValuesMV() {
    return _dataBlockCache.getIntValuesForMVColumn(_column);
  }

  @Override
  public long[][] getLongValuesMV() {
    return _dataBlockCache.getLongValuesForMVColumn(_column);
  }

  @Override
  public float[][] getFloatValuesMV() {
    return _dataBlockCache.getFloatValuesForMVColumn(_column);
  }

  @Override
  public double[][] getDoubleValuesMV() {
    return _dataBlockCache.getDoubleValuesForMVColumn(_column);
  }

  @Override
  public String[][] getStringValuesMV() {
    return _dataBlockCache.getStringValuesForMVColumn(_column);
  }

  @Override
  public int[] getNumMVEntries() {
    return _dataBlockCache.getNumValuesForMVColumn(_column);
  }
}
