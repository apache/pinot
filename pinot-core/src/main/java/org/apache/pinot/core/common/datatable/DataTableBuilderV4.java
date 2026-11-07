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
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.datatable.DataTableImplV4;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.ByteArray;


public class DataTableBuilderV4 extends BaseDataTableBuilder {
  private final Object2IntOpenHashMap<String> _dictionary = new Object2IntOpenHashMap<>();
  /// Lower bound of the size of the serialized dictionary: each entry takes an int length plus at least one UTF-8 byte
  /// per char.
  private long _dictionarySizeBytes;

  public DataTableBuilderV4(DataSchema dataSchema) {
    super(dataSchema, DataTableFactory.VERSION_4);
    _dictionary.defaultReturnValue(-1);
  }

  @Override
  public void setColumn(int colId, String value) {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    int dictId = getDictId(value);
    _currentRowDataByteBuffer.putInt(dictId);
  }

  @Override
  public void setColumn(int colId, ByteArray value)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(variableSizeDataOffset());
    byte[] bytes = value.getBytes();
    _currentRowDataByteBuffer.putInt(bytes.length);
    _variableSizeDataOutputStream.write(bytes);
  }

  @Override
  public void setColumn(int colId, String[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(variableSizeDataOffset());
    _currentRowDataByteBuffer.putInt(values.length);
    for (String value : values) {
      int dictId = getDictId(value);
      _variableSizeDataOutputStream.writeInt(dictId);
    }
  }

  private int getDictId(String value) {
    int dictId = _dictionary.getInt(value);
    if (dictId < 0) {
      dictId = _dictionary.size();
      _dictionary.put(value, dictId);
      _dictionarySizeBytes += Integer.BYTES + value.length();
    }
    return dictId;
  }

  @Override
  protected long getSizeBytes() {
    return super.getSizeBytes() + _dictionarySizeBytes;
  }

  @Override
  public DataTable build() {
    try {
      writeNullBitmaps();
    } catch (IOException e) {
      // Both buffers are in-memory heap pages, so this cannot happen.
      throw new UncheckedIOException(e);
    }
    String[] reverseDictionary = new String[_dictionary.size()];
    for (Object2IntMap.Entry<String> entry : _dictionary.object2IntEntrySet()) {
      reverseDictionary[entry.getIntValue()] = entry.getKey();
    }
    // The table views the written pages directly. Pages are heap memory, so nothing has to be released. The views
    // end at the bytes written so far, and the builder only appends, so later writes do not change this table.
    return new DataTableImplV4(_numRows, _dataSchema, reverseDictionary,
        _fixedSizeDataOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false),
        _variableSizeDataOutputStream.asBuffer(ByteOrder.BIG_ENDIAN, false));
  }
}
