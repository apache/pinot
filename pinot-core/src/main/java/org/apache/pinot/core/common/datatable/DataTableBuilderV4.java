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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.datatable.DataTableImplV4;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


public class DataTableBuilderV4 extends BaseDataTableBuilder {
  private final Object2IntOpenHashMap<String> _dictionary = new Object2IntOpenHashMap<>();

  public DataTableBuilderV4(DataSchema dataSchema) {
    super(dataSchema, DataTableFactory.VERSION_4);
  }

  @Override
  public void setColumn(int colId, String value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    int dictId = _dictionary.computeIntIfAbsent(value, k -> _dictionary.size());
    _currentRowDataByteBuffer.putInt(dictId);
  }

  @Override
  public void setColumn(int colId, ByteArray value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = value.getBytes();
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

  @Override
  public void setColumn(int colId, Vector value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    byte[] bytes = value.toBytes();
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataByteArrayOutputStream.write(bytes);
  }

  @Override
  public void setColumn(int colId, String[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);
    for (String value : values) {
      int dictId = _dictionary.computeIntIfAbsent(value, k -> _dictionary.size());
      _variableSizeDataOutputStream.writeInt(dictId);
    }
  }

  @Override
  public void setNullRowIds(@Nullable RoaringBitmap nullRowIds)
      throws IOException {
    _fixedSizeDataOutputStream.writeInt(_variableSizeDataByteArrayOutputStream.size());
    if (nullRowIds == null || nullRowIds.isEmpty()) {
      _fixedSizeDataOutputStream.writeInt(0);
    } else {
      byte[] bitmapBytes = RoaringBitmapUtils.serialize(nullRowIds);
      _fixedSizeDataOutputStream.writeInt(bitmapBytes.length);
      _variableSizeDataByteArrayOutputStream.write(bitmapBytes);
    }
  }

  @Override
  public DataTable build() {
    String[] reverseDictionary = new String[_dictionary.size()];
    for (Object2IntMap.Entry<String> entry : _dictionary.object2IntEntrySet()) {
      reverseDictionary[entry.getIntValue()] = entry.getKey();
    }
    return new DataTableImplV4(_numRows, _dataSchema, reverseDictionary,
        _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
  }
}
