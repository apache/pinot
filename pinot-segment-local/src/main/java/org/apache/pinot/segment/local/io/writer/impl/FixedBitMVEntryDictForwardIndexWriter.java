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
package org.apache.pinot.segment.local.io.writer.impl;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Bit-compressed dictionary-encoded forward index writer for multi-value columns, where a second level dictionary
 * encoding for multi-value entries (instead of individual values within the entry) are maintained within the forward
 * index.
 *
 * Index layout:
 * - Index header (24 bytes)
 * - ID buffer (stores the multi-value entry id for each doc id)
 * - Offset buffer (stores the start offset of each multi-value entry, followed by end offset of the last value)
 * - Value buffer (stores the individual values)
 *
 * Header layout:
 * - Magic marker (4 bytes)
 * - Version (2 bytes)
 * - Bits per value (1 byte)
 * - Bits per id (1 byte)
 * - Number of unique MV entries (4 bytes)
 * - Number of total values (4 bytes)
 * - Start offset of offset buffer (4 bytes)
 * - Start offset of value buffer (4 bytes)
 */
public class FixedBitMVEntryDictForwardIndexWriter implements Closeable {
  public static final int MAGIC_MARKER = 0xffabcdef;
  public static final short VERSION = 1;
  public static final int HEADER_SIZE = 24;

  private final Object2IntOpenHashMap<IntArrayList> _entryToIdMap = new Object2IntOpenHashMap<>();
  private final File _file;
  private final int _numBitsPerValue;
  private final IntArrayList _ids;

  public FixedBitMVEntryDictForwardIndexWriter(File file, int numDocs, int numBitsPerValue) {
    _file = file;
    _numBitsPerValue = numBitsPerValue;
    _ids = new IntArrayList(numDocs);
  }

  public void putDictIds(int[] dictIds) {
    // Lookup the map, and create a new id when the entry is not found.
    _ids.add(_entryToIdMap.computeIntIfAbsent(IntArrayList.wrap(dictIds), k -> _entryToIdMap.size()));
  }

  @Override
  public void close()
      throws IOException {
    int numUniqueEntries = _entryToIdMap.size();
    int[][] idToDictIdsMap = new int[numUniqueEntries][];
    int numTotalValues = 0;
    for (Object2IntMap.Entry<IntArrayList> entry : _entryToIdMap.object2IntEntrySet()) {
      int id = entry.getIntValue();
      int[] dictIds = entry.getKey().elements();
      idToDictIdsMap[id] = dictIds;
      numTotalValues += dictIds.length;
    }
    int[] ids = _ids.elements();
    int numBitsPerId = PinotDataBitSet.getNumBitsPerValue(numUniqueEntries - 1);
    int idBufferSize = (int) (((long) ids.length * numBitsPerId + 7) / 8);
    int numBitsPerOffset = PinotDataBitSet.getNumBitsPerValue(numTotalValues);
    int offsetBufferSize = (int) (((long) (numUniqueEntries + 1) * numBitsPerOffset + 7) / 8);
    int valueBufferSize = (int) (((long) numTotalValues * _numBitsPerValue + 7) / 8);
    int offsetBufferOffset = HEADER_SIZE + idBufferSize;
    int valueBufferOffset = offsetBufferOffset + offsetBufferSize;
    int indexSize = valueBufferOffset + valueBufferSize;
    try (PinotDataBuffer indexBuffer = PinotDataBuffer.mapFile(_file, false, 0, indexSize, ByteOrder.BIG_ENDIAN,
        getClass().getSimpleName())) {
      indexBuffer.putInt(0, MAGIC_MARKER);
      indexBuffer.putShort(4, VERSION);
      indexBuffer.putByte(6, (byte) _numBitsPerValue);
      indexBuffer.putByte(7, (byte) numBitsPerId);
      indexBuffer.putInt(8, numUniqueEntries);
      indexBuffer.putInt(12, numTotalValues);
      indexBuffer.putInt(16, offsetBufferOffset);
      indexBuffer.putInt(20, valueBufferOffset);

      try (FixedBitIntReaderWriter idWriter = new FixedBitIntReaderWriter(
          indexBuffer.view(HEADER_SIZE, offsetBufferOffset), ids.length, numBitsPerId)) {
        idWriter.writeInt(0, ids.length, ids);
      }
      try (FixedBitIntReaderWriter offsetWriter = new FixedBitIntReaderWriter(
          indexBuffer.view(offsetBufferOffset, valueBufferOffset), numUniqueEntries + 1, numBitsPerOffset);
          FixedBitIntReaderWriter valueWriter = new FixedBitIntReaderWriter(
              indexBuffer.view(valueBufferOffset, indexSize), numTotalValues, _numBitsPerValue)) {
        int startOffset = 0;
        for (int i = 0; i < numUniqueEntries; i++) {
          offsetWriter.writeInt(i, startOffset);
          int[] dictIds = idToDictIdsMap[i];
          valueWriter.writeInt(startOffset, dictIds.length, dictIds);
          startOffset += dictIds.length;
        }
        offsetWriter.writeInt(numUniqueEntries, startOffset);
      }
    }
  }
}
