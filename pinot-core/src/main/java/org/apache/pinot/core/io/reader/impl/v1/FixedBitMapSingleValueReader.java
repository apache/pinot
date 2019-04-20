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
package org.apache.pinot.core.io.reader.impl.v1;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.core.io.reader.BaseMapSingleValueReader;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class FixedBitMapSingleValueReader extends
    BaseMapSingleValueReader<FixedBitMapSingleValueReader.Context> {

  private final FixedBitIntReaderWriter _keyReader;
  private final FixedBitIntReaderWriter _valueReader;
  private final FixedByteSingleValueMultiColReader _headerReader;
  private final PinotDataBuffer _buffer;

  /**
   * @param file output file
   * @param numDocs total number of documents
   */
  public FixedBitMapSingleValueReader(File file, int numDocs, int numTotalEntries, int numKeys,
      int keySizeInBits, int valueSizeInBits) throws Exception {
    //Header Buffer numDocs * int
    //key _buffer -->  numTotalEntries * ((keySizeInBits + Byte.SIZE -1)/8)
    //value _buffer --> numTotalEntries * ((valueSizeInBits + Byte.SIZE -1)/8)

    long headerSize = ((long) numDocs * Integer.BYTES);
    long keyBufferSize = (long) ((numTotalEntries * keySizeInBits + Byte.SIZE - 1) / Byte.SIZE);
    long valueBufferSize = (long) ((numTotalEntries * valueSizeInBits + Byte.SIZE - 1) / Byte.SIZE);
    long offsetBufferSize = (numKeys * numDocs * Integer.BYTES);

    long totalSize = headerSize + keyBufferSize + valueBufferSize + offsetBufferSize;

    _buffer = PinotDataBuffer
        .mapFile(file, false, 0, totalSize, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
    PinotDataBuffer headerBuffer = _buffer.view(0, headerSize);
    _headerReader =
        new FixedByteSingleValueMultiColReader(headerBuffer, 1, new int[]{4});
    PinotDataBuffer keyBuffer = _buffer
        .view(headerSize, headerSize + keyBufferSize, ByteOrder.BIG_ENDIAN);
    PinotDataBuffer valueBuffer = _buffer
        .view(headerSize + keyBufferSize, headerSize + keyBufferSize + valueBufferSize,
            ByteOrder.BIG_ENDIAN);
    _keyReader = new FixedBitIntReaderWriter(keyBuffer, numTotalEntries, keySizeInBits);
    _valueReader = new FixedBitIntReaderWriter(valueBuffer, numTotalEntries, valueSizeInBits);
  }

  @Override
  public int getIntValue(int rowId, int key) {
    int startEntryOffset = 0;
    if (rowId > 0) {
      startEntryOffset = _headerReader.getInt(rowId - 1, 0);
    }
    int endEntryOffset = _headerReader.getInt(rowId, 0);
    ;

    for (int i = startEntryOffset; i < endEntryOffset; i++) {
      int val = _keyReader.readInt(i);
      if (val == key) {
        return _valueReader.readInt(i);
      }
    }
    return -1;
  }

  @Override
  public int getIntIntMap(int rowId, int[] keys, int[] values) {
    int startEntryOffset = 0;
    if (rowId > 0) {
      startEntryOffset = _headerReader.getInt(rowId - 1, 0);
    }
    int endEntryOffset = _headerReader.getInt(rowId, 0);

    for (int i = startEntryOffset, index = 0; i < endEntryOffset && index < keys.length;
        i++, index++) {
      keys[index] = _keyReader.readInt(i);
      values[index] = _valueReader.readInt(i);
    }
    return endEntryOffset - startEntryOffset;
  }

  @Override
  public void close() throws IOException {
    if (_buffer != null) {
      _buffer.close();
    }
  }

  public class Context implements ReaderContext {

  }
}
