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


public class StarTreeDataTableOptimized {
  Logger LOGGER = LoggerFactory.getLogger(StarTreeDataTableOptimized.class);

  private File file;
  final MMapBuffer mappedByteBuffer;

  private int dimensionSizeInBytes;
  private int metricSizeInBytes;
  private int totalSizeInBytes;

  public StarTreeDataTableOptimized(File file, int dimensionSizeInBytes, int metricSizeInBytes)
      throws IOException {
    this.file = file;
    this.dimensionSizeInBytes = dimensionSizeInBytes;
    this.metricSizeInBytes = metricSizeInBytes;
    this.totalSizeInBytes = dimensionSizeInBytes + metricSizeInBytes;
    mappedByteBuffer = new MMapBuffer(file, 0, file.length(), MMapMode.READ_WRITE);
  }

  /**
   * @param startRecordId inclusive
   * @param endRecordId exclusive
   * @param sortOrder
   */
  public void sort(int startRecordId, int endRecordId, final int[] sortOrder) {
    int length = endRecordId - startRecordId;
    final int startOffset = startRecordId * totalSizeInBytes;

    List<Integer> idList = new ArrayList<Integer>();
    for (int i = startRecordId; i < endRecordId; i++) {
      idList.add(i - startRecordId);
    }

    Comparator<Integer> comparator = new Comparator<Integer>() {
      byte[] buf1 = new byte[dimensionSizeInBytes];
      byte[] buf2 = new byte[dimensionSizeInBytes];

      @Override
      public int compare(Integer o1, Integer o2) {
        int pos1 = startOffset + (o1) * totalSizeInBytes;
        int pos2 = startOffset + (o2) * totalSizeInBytes;

        mappedByteBuffer.copyTo(pos1, buf1, 0, dimensionSizeInBytes);
        mappedByteBuffer.copyTo(pos2, buf2, 0, dimensionSizeInBytes);

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
      mappedByteBuffer.copyTo(startOffset + thisRecordIdPos * totalSizeInBytes, buf1, 0, totalSizeInBytes);
      mappedByteBuffer.copyTo(startOffset + thatRecordIdPos * totalSizeInBytes, buf2, 0, totalSizeInBytes);
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
   * @param startDocId inclusive
   * @param endDocId exclusive
   * @param colIndex
   * @return start, end for each value. inclusive start, exclusive end
   */
  public Map<Integer, IntPair> groupByIntColumnCount(int startDocId, int endDocId, Integer colIndex) {
    int length = endDocId - startDocId;
    Map<Integer, IntPair> rangeMap = new LinkedHashMap<>();
    final int startOffset = startDocId * totalSizeInBytes;

    int prevValue = -1;
    int prevStart = 0;
    byte[] dimBuff = new byte[dimensionSizeInBytes];

    for (int i = 0; i < length; i++) {
      mappedByteBuffer.copyTo(startOffset + (i * totalSizeInBytes), dimBuff, 0, dimensionSizeInBytes);
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
  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId)
      throws IOException {
    try {
      final int length = endDocId - startDocId;
      final int startOffset = startDocId * totalSizeInBytes;
      final MMapBuffer mappedByteBuffer =
          new MMapBuffer(file, startOffset, length * totalSizeInBytes, MMapMode.READ_WRITE);
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

          mappedByteBuffer.copyTo(pointer * totalSizeInBytes, dimBuff, 0, dimensionSizeInBytes);
          if (metricSizeInBytes > 0) {
            mappedByteBuffer.copyTo(pointer * totalSizeInBytes + dimensionSizeInBytes, metBuff, 0, metricSizeInBytes);
          }

          pointer = pointer + 1;
          if (pointer == length) {
            try {
              mappedByteBuffer.close();
            } catch (IOException e) {
              LOGGER.error("Exception caught while trying to close byte-buffer", e);
            }
          }

          return Pair.of(dimBuff, metBuff);
        }
      };
    } catch (IOException e) {
      throw e;
    }
  }
}
