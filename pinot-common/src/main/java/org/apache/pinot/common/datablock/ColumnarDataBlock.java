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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/**
 * Column-wise data table. It stores data in columnar-major format.
 */
public class ColumnarDataBlock extends BaseDataBlock {
  protected int[] _cumulativeColumnOffsetSizeInBytes;
  protected int[] _columnSizeInBytes;
  private int _fixDataSize;

  public ColumnarDataBlock(int numRows, DataSchema dataSchema, String[] stringDictionary,
      DataBuffer fixedSizeDataBytes, DataBuffer variableSizeDataBytes) {
    super(numRows, dataSchema, stringDictionary, fixedSizeDataBytes, variableSizeDataBytes);
    computeBlockObjectConstants();
  }

  protected void computeBlockObjectConstants() {
    _fixDataSize = 0;
    if (_dataSchema != null) {
      _cumulativeColumnOffsetSizeInBytes = new int[_numColumns];
      _columnSizeInBytes = new int[_numColumns];
      DataBlockUtils.computeColumnSizeInBytes(_dataSchema, _columnSizeInBytes);
      int cumulativeColumnOffset = 0;
      for (int i = 0; i < _numColumns; i++) {
        _cumulativeColumnOffsetSizeInBytes[i] = cumulativeColumnOffset;
        cumulativeColumnOffset += _columnSizeInBytes[i] * _numRows;
      }
      _fixDataSize = cumulativeColumnOffset;
    }
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    return _cumulativeColumnOffsetSizeInBytes[colId] + _columnSizeInBytes[colId] * rowId;
  }

  @Override
  public Type getDataBlockType() {
    return Type.COLUMNAR;
  }

  @Override
  protected int getFixDataSize() {
    return _fixDataSize;
  }

// TODO: add whole-column access methods.
}
