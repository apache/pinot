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
package org.apache.pinot.core.io.writer.impl.v1;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map;
import org.apache.pinot.core.io.readerwriter.impl.FixedByteSingleValueMultiColumnReaderWriter;
import org.apache.pinot.core.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.core.io.writer.MapSingleValueWriter;
import org.apache.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;

/**
 * Writer to be used in Batch Mode where keys and values are dictionary encoded.
 */
public class FixedBitMapSingleValueWriter extends BaseMapSingleValueWriter {


  private final FixedBitIntReaderWriter keyWriter;
  private final FixedBitIntReaderWriter valueWriter;
  private final PinotDataBuffer buffer;
  private final FixedByteSingleValueMultiColWriter headerWriter;

  int currentRowId = -1;

  int currentEntryId = 0;

  /**
   * @param file output file
   * @param numDocs total number of documents
   */
  public FixedBitMapSingleValueWriter(File file, int numDocs, int numTotalEntries, int numKeys,
      int keySizeInBits, int valueSizeInBits) throws Exception {
    //Header Buffer numDocs * int
    //key buffer -->  numTotalEntries * ((keySizeInBits + Byte.SIZE -1)/8)
    //value buffer --> numTotalEntries * ((valueSizeInBits + Byte.SIZE -1)/8)
    //offset buffer --> numKeys * numDocs

    long headerSize = (long) numDocs * Integer.BYTES;
    long keyBufferSize = (long) ((numTotalEntries * keySizeInBits + Byte.SIZE - 1) / Byte.SIZE);
    long valueBufferSize = (long) ((numTotalEntries * valueSizeInBits + Byte.SIZE - 1) / Byte.SIZE);
//    long offsetBufferSize = (numKeys * numDocs * Integer.BYTES);

    long totalSize = headerSize + keyBufferSize + valueBufferSize;

    buffer = PinotDataBuffer
        .mapFile(file, false, 0, totalSize, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
    PinotDataBuffer headerBuffer = buffer.view(0, headerSize);

    headerWriter =
        new FixedByteSingleValueMultiColWriter(headerBuffer, 1, new int[]{4});
    PinotDataBuffer keyBuffer = buffer
        .view(headerSize, headerSize + keyBufferSize, ByteOrder.BIG_ENDIAN);
    PinotDataBuffer valueBuffer = buffer
        .view(headerSize + keyBufferSize, headerSize + keyBufferSize + valueBufferSize,
            ByteOrder.BIG_ENDIAN);
    keyWriter = new FixedBitIntReaderWriter(keyBuffer, numTotalEntries, keySizeInBits);
    valueWriter = new FixedBitIntReaderWriter(valueBuffer, numTotalEntries, valueSizeInBits);
    PinotDataBuffer offsetBuffer = buffer
        .view(headerSize + keyBufferSize + valueBufferSize, totalSize);
  }

  @Override
  public void setIntIntMap(int row, Map<Integer, Integer> map) {
    currentRowId = row;
    for (Integer key : map.keySet()) {
      Preconditions.checkArgument(row == currentRowId, "Invoke startRow before setting key/value");
      keyWriter.writeInt(currentEntryId, key);
      valueWriter.writeInt(currentEntryId, map.get(key));
//    offsetBuffer.putInt(keyDictId * numDocs + row, currentEntryId);
      currentEntryId = currentEntryId + 1;
    }
    headerWriter.setInt(row, 0, currentEntryId);
  }

  @Override
  public void close() throws IOException {
    headerWriter.close();
    keyWriter.close();
    valueWriter.close();
    buffer.close();
  }
}
