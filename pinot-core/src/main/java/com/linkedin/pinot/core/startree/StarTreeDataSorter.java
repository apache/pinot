/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.utils.Pairs.IntPair;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

/**
 * A star tree data table optimized for in-place modifications/sorting
 * of data. This class allows for the following operations on a fixed size data table:
 * - Sort all or range of data.
 * - Group by column count
 * - Iterate over dimension/metric buffers.
 */
public class StarTreeDataSorter {
  Logger LOGGER = LoggerFactory.getLogger(StarTreeDataSorter.class);

  final MMapBuffer mappedByteBuffer;

  private int dimensionSizeInBytes;
  private int metricSizeInBytes;
  private int totalSizeInBytes;

  public StarTreeDataSorter(File file, int dimensionSizeInBytes, int metricSizeInBytes) throws IOException {
    this.dimensionSizeInBytes = dimensionSizeInBytes;
    this.metricSizeInBytes = metricSizeInBytes;
    this.totalSizeInBytes = dimensionSizeInBytes + metricSizeInBytes;
    mappedByteBuffer = new MMapBuffer(file, 0, file.length(), MMapMode.READ_WRITE);
  }

  /**
   * Sort from to given start (inclusive) to end (exclusive) as per the provided sort order.
   *
   * @param startRecordId inclusive
   * @param endRecordId exclusive
   * @param sortOrder
   */
  public void sort(int startRecordId, int endRecordId, final int[] sortOrder) {
    int length = endRecordId - startRecordId;
    final long startOffset = (long) startRecordId * totalSizeInBytes;

    List<Integer> idList = new ArrayList<Integer>();
    for (int i = startRecordId; i < endRecordId; i++) {
      idList.add(i - startRecordId);
    }

    Comparator<Integer> comparator = new Comparator<Integer>() {
      byte[] buf1 = new byte[dimensionSizeInBytes];
      byte[] buf2 = new byte[dimensionSizeInBytes];

      @Override
      public int compare(Integer o1, Integer o2) {
        long pos1 = startOffset + (o1) * totalSizeInBytes;
        long pos2 = startOffset + (o2) * totalSizeInBytes;

        copyTo(pos1, buf1, 0, dimensionSizeInBytes);
        copyTo(pos2, buf2, 0, dimensionSizeInBytes);

        IntBuffer bb1 = ByteBuffer.wrap(buf1).asIntBuffer();
        IntBuffer bb2 = ByteBuffer.wrap(buf2).asIntBuffer();

        for (int dimIndex : sortOrder) {
          int v1 = bb1.get(dimIndex);
          int v2 = bb2.get(dimIndex);
          if (v1 != v2) {
            return v1 - v2;
          }
        }
        return 0;
      }
    };

    Collections.sort(idList, comparator);
    int[] currentPositions = new int[length];
    int[] indexToRecordIdMapping = new int[length];

    byte[] buf1 = new byte[totalSizeInBytes];
    byte[] buf2 = new byte[totalSizeInBytes];

    for (int i = 0; i < length; i++) {
      currentPositions[i] = i;
      indexToRecordIdMapping[i] = i;
    }

    for (int i = 0; i < length; i++) {
      int thisRecordId = indexToRecordIdMapping[i];
      int thisRecordIdPos = currentPositions[thisRecordId];

      int thatRecordId = idList.get(i);
      int thatRecordIdPos = currentPositions[thatRecordId];

      // swap the buffers
      copyTo(startOffset + thisRecordIdPos * totalSizeInBytes, buf1, 0, totalSizeInBytes);
      copyTo(startOffset + thatRecordIdPos * totalSizeInBytes, buf2, 0, totalSizeInBytes);
      mappedByteBuffer.readFrom(buf2, 0, startOffset + thisRecordIdPos * totalSizeInBytes, totalSizeInBytes);
      mappedByteBuffer.readFrom(buf1, 0, startOffset + thatRecordIdPos * totalSizeInBytes, totalSizeInBytes);

      // update the positions
      indexToRecordIdMapping[i] = thatRecordId;
      indexToRecordIdMapping[thatRecordIdPos] = thisRecordId;

      currentPositions[thatRecordId] = i;
      currentPositions[thisRecordId] = thatRecordIdPos;
    }
  }

  /**
   * Close the mappedByteBuffer
   */
  public void close() {
    if (mappedByteBuffer != null) {
      mappedByteBuffer.flush();
      try {
        mappedByteBuffer.close();
      } catch (IOException e) {
        LOGGER.error("Exception caught while trying to close byte-buffer", e);
      }
    }
  }

  /**
   * Perform group-by based on the 'count' for the given column.
   *
   * @param startDocId inclusive
   * @param endDocId exclusive
   * @param colIndex
   * @return start, end for each value. inclusive start, exclusive end
   */
  public Map<Integer, IntPair> groupByIntColumnCount(int startDocId, int endDocId, Integer colIndex) {
    int length = endDocId - startDocId;
    Map<Integer, IntPair> rangeMap = new LinkedHashMap<>();
    final long startOffset = (long) startDocId * totalSizeInBytes;

    int prevValue = -1;
    int prevStart = 0;
    byte[] dimBuff = new byte[dimensionSizeInBytes];

    for (int i = 0; i < length; i++) {
      copyTo(startOffset + (i * totalSizeInBytes), dimBuff, 0, dimensionSizeInBytes);
      int value = ByteBuffer.wrap(dimBuff).asIntBuffer().get(colIndex);
      if (prevValue != -1 && prevValue != value) {
        rangeMap.put(prevValue, new IntPair(startDocId + prevStart, startDocId + i));
        prevStart = i;
      }
      prevValue = value;
    }

    rangeMap.put(prevValue, new IntPair(startDocId + prevStart, endDocId));
    return rangeMap;
  }

  /**
   * Returns iterator over dimension and metric byte buffers.
   *
   * @param startDocId
   * @param endDocId
   * @return
   * @throws IOException
   */
  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId) throws IOException {
    final int length = endDocId - startDocId;
    final long startOffset = (long) startDocId * totalSizeInBytes;
    return new Iterator<Pair<byte[], byte[]>>() {
      int pointer = 0;

      @Override
      public boolean hasNext() {
        return pointer < length;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Pair<byte[], byte[]> next() {
        byte[] dimBuff = new byte[dimensionSizeInBytes];
        byte[] metBuff = new byte[metricSizeInBytes];

        long pointerOffset = (long) pointer * totalSizeInBytes;
        copyTo(startOffset + pointerOffset, dimBuff, 0, dimensionSizeInBytes);
        if (metricSizeInBytes > 0) {
          copyTo(startOffset + pointerOffset + dimensionSizeInBytes, metBuff, 0, metricSizeInBytes);
        }

        pointer = pointer + 1;
        return Pair.of(dimBuff, metBuff);
      }
    };
  }

  /**
   * Copied from LArray code and modified to support long offsets
   * TODO:Remove this once the method is available in Larray library
   * @param srcOffset
   * @param destArray
   * @param destOffset
   * @param size
   */
  private void copyTo(long srcOffset, byte[] destArray, int destOffset, int size) {
    if (srcOffset < 0) {
      throw new ArrayIndexOutOfBoundsException("Invalid offset:" + srcOffset);
    }
    int cursor = destOffset;
    for (ByteBuffer bb : mappedByteBuffer.toDirectByteBuffers(srcOffset, size)) {
      int bbSize = bb.remaining();
      if ((cursor + bbSize) > destArray.length) {
        throw new ArrayIndexOutOfBoundsException(String.format("cursor + bbSize = %,d", cursor + bbSize));
      }
      bb.get(destArray, cursor, bbSize);
      cursor += bbSize;
    }
  }
}
