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
package org.apache.pinot.query.runtime.operator.block;

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;

/**
 * In the multistage engine, the leaf stage servers process the data in columnar fashion. By the time the
 * intermediate stage receives the projected column, they are converted to a row based format. This class provides
 * the capability to convert the row based representation into columnar blocks so that they can be used to process
 * aggregations using v1 aggregation functions.
 * TODO: Support MV
 */
public class DataBlockValSet implements BlockValSet {
  protected final FieldSpec.DataType _dataType;
  protected final DataBlock _dataBlock;
  protected final int _index;
  protected final RoaringBitmap _nullBitMap;

  public DataBlockValSet(DataSchema.ColumnDataType columnDataType, DataBlock dataBlock, int colIndex) {
    _dataType = columnDataType.toDataType();
    _dataBlock = dataBlock;
    _index = colIndex;
    _nullBitMap = dataBlock.getNullRowIds(colIndex);
  }

  /**
   * Returns a bitmap of indices where null values are found.
   */
  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return _nullBitMap;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    // TODO: Needs to be changed when we start supporting MV in multistage
    return true;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    return DataBlockUtils.extractIntValuesForColumn(_dataBlock, _index);
  }

  @Override
  public long[] getLongValuesSV() {
    return DataBlockUtils.extractLongValuesForColumn(_dataBlock, _index);
  }

  @Override
  public float[] getFloatValuesSV() {
    return DataBlockUtils.extractFloatValuesForColumn(_dataBlock, _index);
  }

  @Override
  public double[] getDoubleValuesSV() {
    return DataBlockUtils.extractDoubleValuesForColumn(_dataBlock, _index);
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    return DataBlockUtils.extractBigDecimalValuesForColumn(_dataBlock, _index);
  }

  @Override
  public String[] getStringValuesSV() {
    return DataBlockUtils.extractStringValuesForColumn(_dataBlock, _index);
  }

  @Override
  public byte[][] getBytesValuesSV() {
    return DataBlockUtils.extractBytesValuesForColumn(_dataBlock, _index);
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] getLongValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] getStringValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException();
  }
}
