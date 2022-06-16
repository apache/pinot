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

package org.apache.pinot.core.common.datatable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.datablock.RowDataBlock;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.roaringbitmap.RoaringBitmap;


/**
 * Datatable V4 Implementation is a wrapper around the Row-based data block.
 */
@InterfaceStability.Evolving
public class DataTableImplV4 extends RowDataBlock {

  public DataTableImplV4() {
    super();
  }

  public DataTableImplV4(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
  }

  public DataTableImplV4(int numRows, DataSchema dataSchema, Map<String, Map<Integer, String>> dictionaryMap,
      byte[] fixedSizeDataBytes, byte[] variableSizeDataBytes) {
    super(numRows, dataSchema, dictionaryMap, fixedSizeDataBytes, variableSizeDataBytes);
  }

  @Override
  public RoaringBitmap getNullRowIds(int colId) {
    // _fixedSizeData stores two ints per col's null bitmap: offset, and length.
    int position = _numRows * _rowSizeInBytes + colId * Integer.BYTES * 2;
    _fixedSizeData.position(position);
    int offset = _fixedSizeData.getInt();
    int bytesLength = _fixedSizeData.getInt();
    RoaringBitmap nullBitmap;
    if (bytesLength > 0) {
      _variableSizeData.position(offset);
      byte[] nullBitmapBytes = new byte[bytesLength];
      _variableSizeData.get(nullBitmapBytes);
      nullBitmap = ObjectSerDeUtils.ROARING_BITMAP_SER_DE.deserialize(nullBitmapBytes);
    } else {
      nullBitmap = new RoaringBitmap();
    }
    return nullBitmap;
  }

  @Override
  public int getDataBlockVersionType() {
    return DataTableBuilder.VERSION_4;
  }
}
