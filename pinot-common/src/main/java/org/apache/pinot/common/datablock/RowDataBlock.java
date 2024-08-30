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
package org.apache.pinot.common.datablock;

import javax.annotation.Nonnull;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/**
 * Wrapper for row-wise data table. It stores data in row-major format.
 */
public class RowDataBlock extends BaseDataBlock {
  private static final int VERSION = 2;
  protected int[] _columnOffsets;
  protected int _rowSizeInBytes;
  private int _fixDataSize;

  public RowDataBlock(int numRows, DataSchema dataSchema, String[] stringDictionary,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    super(numRows, dataSchema, stringDictionary, fixedSizeDataBytes, variableSizeDataBytes);
    computeBlockObjectConstants();
  }

  public RowDataBlock(int numRows, DataSchema dataSchema, String[] stringDictionary,
      DataBuffer fixedSizeDataBytes, DataBuffer variableSizeDataBytes) {
    super(numRows, dataSchema, stringDictionary, fixedSizeDataBytes, variableSizeDataBytes);
    computeBlockObjectConstants();
  }

  protected void computeBlockObjectConstants() {
    if (_dataSchema != null) {
      _columnOffsets = new int[_numColumns];
      _rowSizeInBytes = DataBlockUtils.computeColumnOffsets(_dataSchema, _columnOffsets);
      _fixDataSize = _numRows * _rowSizeInBytes;
    }
  }

  @Override
  protected int getFixDataSize() {
    return _fixDataSize;
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    return rowId * _rowSizeInBytes + _columnOffsets[colId];
  }

  public int getRowSizeInBytes() {
    return _rowSizeInBytes;
  }

  @Override
  public Type getDataBlockType() {
    return Type.ROW;
  }

  @Nonnull // the method is override just to override its nullability annotation
  @Override
  public DataSchema getDataSchema() {
    return super.getDataSchema();
  }

// TODO: add whole-row access methods.
}
