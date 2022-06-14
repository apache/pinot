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
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Wrapper for row-wise data table. It stores data in row-major format.
 */
public class RowDataBlock extends BaseDataBlock {
  private static final int VERSION = 1;
  protected int[] _columnOffsets;
  protected int _rowSizeInBytes;

  public RowDataBlock() {
    super();
  }

  public RowDataBlock(int numRows, DataSchema dataSchema, Map<String, Map<Integer, String>> dictionaryMap,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    super(numRows, dataSchema, dictionaryMap, fixedSizeDataBytes, variableSizeDataBytes);
    computeBlockObjectConstants();
  }

  public RowDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
    computeBlockObjectConstants();
  }

  protected void computeBlockObjectConstants() {
    if (_dataSchema != null) {
      _columnOffsets = new int[_numColumns];
      _rowSizeInBytes = DataBlockUtils.computeColumnOffsets(_dataSchema, _columnOffsets);
    }
  }

  @Override
  protected int getDataBlockVersionType() {
    return VERSION + (Type.ROW.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);
  }

  @Override
  protected void positionCursorInFixSizedBuffer(int rowId, int colId) {
    int position = rowId * _rowSizeInBytes + _columnOffsets[colId];
    _fixedSizeData.position(position);
  }

  @Override
  protected int positionCursorInVariableBuffer(int rowId, int colId) {
    positionCursorInFixSizedBuffer(rowId, colId);
    _variableSizeData.position(_fixedSizeData.getInt());
    return _fixedSizeData.getInt();
  }

  @Override
  public RowDataBlock toMetadataOnlyDataTable() {
    RowDataBlock metadataOnlyDataTable = new RowDataBlock();
    metadataOnlyDataTable._metadata.putAll(_metadata);
    metadataOnlyDataTable._errCodeToExceptionMap.putAll(_errCodeToExceptionMap);
    return metadataOnlyDataTable;
  }

  @Override
  public RowDataBlock toDataOnlyDataTable() {
    return new RowDataBlock(_numRows, _dataSchema, _dictionaryMap, _fixedSizeDataBytes, _variableSizeDataBytes);
  }

  // TODO: add whole-row access methods.
}
