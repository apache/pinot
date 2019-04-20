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
package org.apache.pinot.core.io.readerwriter.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.pinot.core.io.readerwriter.BaseMapSingleValueReaderWriter;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;

public class FixedByteMapSingleValueReaderWriter extends BaseMapSingleValueReaderWriter {

  private int _entryCount = 0;
  private FixedByteSingleColumnSingleValueReaderWriter _keyReaderWriter;
  private FixedByteSingleColumnSingleValueReaderWriter _valueReaderWriter;
  private FixedByteSingleColumnSingleValueReaderWriter _offsetReaderWriter;

  public FixedByteMapSingleValueReaderWriter(int numRowsPerChunk, int avgNumEntriesPerRow,
      int keySizeInBytes, int valueSizeInBytes,
      PinotDataBufferMemoryManager memoryManager, String context) {

    if (avgNumEntriesPerRow <= 0) {
      avgNumEntriesPerRow = 3;
    }

    _keyReaderWriter = new FixedByteSingleColumnSingleValueReaderWriter(
        numRowsPerChunk * avgNumEntriesPerRow,
        keySizeInBytes, memoryManager, context);
    _valueReaderWriter = new FixedByteSingleColumnSingleValueReaderWriter(
        numRowsPerChunk * avgNumEntriesPerRow,
        valueSizeInBytes, memoryManager, context);

    _offsetReaderWriter = new FixedByteSingleColumnSingleValueReaderWriter(
        numRowsPerChunk, Integer.BYTES, memoryManager, context);
  }

  @Override
  public void setIntIntMap(int row, Map<Integer, Integer> map) {
    for (Entry<Integer, Integer> entry : map.entrySet()) {
      _keyReaderWriter.setInt(_entryCount, entry.getKey());
      _valueReaderWriter.setInt(_entryCount, entry.getValue());
      _entryCount++;
    }
    _offsetReaderWriter.setInt(row, _entryCount);
  }

  @Override
  public int getIntKeySet(int rowId, int[] keys) {
    int startEntryOffset = 0;
    if (rowId > 0) {
      startEntryOffset = _offsetReaderWriter.getInt(rowId - 1);
    }
    int endEntryOffset = _offsetReaderWriter.getInt(rowId);
    for (int i = startEntryOffset, index = 0; i < endEntryOffset && index < keys.length;
        i++, index++) {
      keys[index] = _keyReaderWriter.getInt(i);
    }
    return endEntryOffset - startEntryOffset;
  }

  @Override
  public int getIntIntMap(int rowId, int[] keys, int[] values) {
    int startEntryOffset = 0;
    if (rowId > 0) {
      startEntryOffset = _offsetReaderWriter.getInt(rowId - 1);
    }
    int endEntryOffset = _offsetReaderWriter.getInt(rowId);

    for (int i = startEntryOffset, index = 0; i < endEntryOffset && index < keys.length;
        i++, index++) {
      keys[index] = _keyReaderWriter.getInt(i);
      values[index] = _valueReaderWriter.getInt(i);
    }
    return endEntryOffset - startEntryOffset;
  }

  @Override
  public int getIntValue(int row, int key) {
    int startOffset = 0;
    if (row > 0) {
      startOffset = _offsetReaderWriter.getInt(row - 1);
    }
    int endOffset = _offsetReaderWriter.getInt(row);
    for (int i = startOffset; i < endOffset; i++) {
      if (key == _keyReaderWriter.getInt(i)) {
        return _valueReaderWriter.getInt(i);
      }
    }
    return -1;
  }

  @Override
  public void close() throws IOException {
    _keyReaderWriter.close();
    _valueReaderWriter.close();
    _offsetReaderWriter.close();
  }
}
