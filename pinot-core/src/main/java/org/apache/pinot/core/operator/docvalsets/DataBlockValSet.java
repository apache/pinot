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

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.RoaringBitmap;


/**
 * In the multistage engine, the leaf stage servers process the data in columnar fashion. By the time the
 * intermediate stage receives the projected column, they are converted to a row based format. This class provides
 * the capability to convert the row based representation into columnar blocks so that they can be used to process
 * aggregations using v1 aggregation functions.
 * TODO: Support MV
 */
public class DataBlockValSet implements BlockValSet {
  private final DataType _dataType;
  private final DataType _storedType;
  private final DataBlock _dataBlock;
  private final int _colId;
  private final RoaringBitmap _nullBitmap;

  public DataBlockValSet(ColumnDataType columnDataType, DataBlock dataBlock, int colId) {
    _dataType = columnDataType.toDataType();
    _storedType = _dataType.getStoredType();
    _dataBlock = dataBlock;
    _colId = colId;
    _nullBitmap = dataBlock.getNullRowIds(colId);
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return _nullBitmap;
  }

  @Override
  public DataType getValueType() {
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
    return DataBlockExtractUtils.extractIntColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public long[] getLongValuesSV() {
    return DataBlockExtractUtils.extractLongColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public float[] getFloatValuesSV() {
    return DataBlockExtractUtils.extractFloatColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public double[] getDoubleValuesSV() {
    return DataBlockExtractUtils.extractDoubleColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    return DataBlockExtractUtils.extractBigDecimalColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public Vector[] getVectorValuesSV() {
    return DataBlockExtractUtils.extractVectorValuesForColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public String[] getStringValuesSV() {
    return DataBlockExtractUtils.extractStringColumn(_storedType, _dataBlock, _colId, _nullBitmap);
  }

  @Override
  public byte[][] getBytesValuesSV() {
    return DataBlockExtractUtils.extractBytesColumn(_storedType, _dataBlock, _colId, _nullBitmap);
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
