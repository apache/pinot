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
package org.apache.pinot.core.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Column-wise data table. It stores data in columnar-major format.
 */
public class ColumnarDataBlock extends BaseDataBlock {
  private static final int VERSION = 1;
  protected int[] _cumulativeColumnOffsetSizeInBytes;
  protected int[] _columnSizeInBytes;

  public ColumnarDataBlock() {
    super();
  }

  public ColumnarDataBlock(int numRows, DataSchema dataSchema, String[] stringDictionary,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    super(numRows, dataSchema, stringDictionary, fixedSizeDataBytes, variableSizeDataBytes);
    computeBlockObjectConstants();
  }

  public ColumnarDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
    computeBlockObjectConstants();
  }

  protected void computeBlockObjectConstants() {
    if (_dataSchema != null) {
      _cumulativeColumnOffsetSizeInBytes = new int[_numColumns];
      _columnSizeInBytes = new int[_numColumns];
      DataBlockUtils.computeColumnSizeInBytes(_dataSchema, _columnSizeInBytes);
      int cumulativeColumnOffset = 0;
      for (int i = 0; i < _numColumns; i++) {
        _cumulativeColumnOffsetSizeInBytes[i] = cumulativeColumnOffset;
        cumulativeColumnOffset += _columnSizeInBytes[i] * _numRows;
      }
    }
  }

  @Override
  protected int getDataBlockVersionType() {
    return VERSION + (Type.COLUMNAR.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    return _cumulativeColumnOffsetSizeInBytes[colId] + _columnSizeInBytes[colId] * rowId;
  }

  @Override
  protected int positionOffsetInVariableBufferAndGetLength(int rowId, int colId) {
    int offset = getOffsetInFixedBuffer(rowId, colId);
    _variableSizeData.position(_fixedSizeData.getInt(offset));
    return _fixedSizeData.getInt(offset + 4);
  }

  @Override
  public ColumnarDataBlock toMetadataOnlyDataTable() {
    ColumnarDataBlock metadataOnlyDataTable = new ColumnarDataBlock();
    metadataOnlyDataTable._metadata.putAll(_metadata);
    metadataOnlyDataTable._errCodeToExceptionMap.putAll(_errCodeToExceptionMap);
    return metadataOnlyDataTable;
  }

  @Override
  public ColumnarDataBlock toDataOnlyDataTable() {
    return new ColumnarDataBlock(_numRows, _dataSchema, _stringDictionary, _fixedSizeDataBytes, _variableSizeDataBytes);
  }

  // TODO: add whole-column access methods.
}
