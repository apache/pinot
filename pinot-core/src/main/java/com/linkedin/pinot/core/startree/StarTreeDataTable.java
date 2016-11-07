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
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Iterator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * The StarTreeDataTable should be able to handle the memory range greater than 2GB.
 * As a result, all fields related to memory position should be declared as long to avoid int overflow.
 */
public class StarTreeDataTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeDataTable.class);

  private static final Int2ObjectMap<IntPair> EMPTY_INT_OBJECT_MAP = new Int2ObjectLinkedOpenHashMap<>();
  private static final ByteOrder nativeByteOrder = ByteOrder.nativeOrder();

  private File file;
  private int dimensionSizeInBytes;
  private int metricSizeInBytes;
  private int totalSizeInBytes;
  final int[] sortOrder;

  // Re-usable buffers
  private LBuffer dimLbuf1;
  private LBuffer dimLbuf2;
  private LBufferAPI dimMetLbuf1;
  private LBufferAPI dimMetLbuf2;

  public StarTreeDataTable(File file, int dimensionSizeInBytes, int metricSizeInBytes, int[] sortOrder) {
    this.file = file;
    this.dimensionSizeInBytes = dimensionSizeInBytes;
    this.metricSizeInBytes = metricSizeInBytes;
    this.sortOrder = sortOrder;
    this.totalSizeInBytes = dimensionSizeInBytes + metricSizeInBytes;

    dimLbuf1 = new LBuffer(dimensionSizeInBytes);
    dimLbuf2 = new LBuffer(dimensionSizeInBytes);
    dimMetLbuf1 = new LBuffer(totalSizeInBytes);
    dimMetLbuf2 = new LBuffer(totalSizeInBytes);
  }

  /**
   *
   * @param startRecordId inclusive
   * @param endRecordId exclusive
   */
  public void sort(int startRecordId, int endRecordId) {
    final MMapBuffer mappedByteBuffer;
    try {
      int numRecords = endRecordId - startRecordId;
      final long startOffset = startRecordId * (long) totalSizeInBytes;

      // Sort the docIds without actually moving the docs themselves.
      mappedByteBuffer = new MMapBuffer(file, startOffset, numRecords * (long) totalSizeInBytes, MMapMode.READ_WRITE);
      final int[] sortedDocIds = getSortedDocIds(mappedByteBuffer, totalSizeInBytes, dimensionSizeInBytes, numRecords);

      // Re-arrange the docs as per the sorted docId order.
      sortMmapBuffer(mappedByteBuffer, totalSizeInBytes, numRecords, sortedDocIds);
    } catch (IOException e) {
      LOGGER.error("Exception caught while sorting records", e);
    }
  }

  /**
   * Helper method that returns an array of docIds sorted as per dimension sort order.
   *
   * @param mappedByteBuffer Mmap buffer containing docs to sort
   * @param recordSizeInBytes Size of one record in bytes
   * @param dimensionSizeInBytes Size of dimension columns in bytes
   * @param numRecords Number of records
   * @return DocId array in sorted order
   */
  private int[] getSortedDocIds(final MMapBuffer mappedByteBuffer, final long recordSizeInBytes,
      final long dimensionSizeInBytes, int numRecords) {
    final int[] ids = new int[numRecords];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        long pos1 = (ids[i1]) * recordSizeInBytes;
        long pos2 = (ids[i2]) * recordSizeInBytes;

        mappedByteBuffer.copyTo(pos1, dimLbuf1, 0, dimensionSizeInBytes);
        mappedByteBuffer.copyTo(pos2, dimLbuf2, 0, dimensionSizeInBytes);

        for (int dimIndex : sortOrder) {
          int v1 = flipEndiannessIfNeeded(dimLbuf1.getInt(dimIndex * V1Constants.Numbers.INTEGER_SIZE));
          int v2 = flipEndiannessIfNeeded(dimLbuf2.getInt(dimIndex * V1Constants.Numbers.INTEGER_SIZE));
          if (v1 != v2) {
            return v1 - v2;
          }
        }
        return 0;
      }

      @Override
      public int compare(Integer o1, Integer o2) {
        return compare(o1.intValue(), o2.intValue());
      }
    };

    Swapper swapper = new Swapper() {
      @Override
      public void swap(int i, int j) {
        int tmp = ids[i];
        ids[i] = ids[j];
        ids[j] = tmp;
      }
    };
    Arrays.quickSort(0, numRecords, comparator, swapper);
    return ids;
  }

  /**
   * Helper method to re-arrange the given MMap buffer as per the sorted docId order.
   *
   * @param mappedByteBuffer Mmap buffer to re-arrange
   * @param recordSizeInBytes Size of one record in bytes
   * @param numRecords Total number of records
   * @param sortedDocIds Sorted docId array
   * @throws IOException
   */
  private void sortMmapBuffer(MMapBuffer mappedByteBuffer, long recordSizeInBytes, int numRecords, int[] sortedDocIds)
      throws IOException {
    int[] currentPositions = new int[numRecords];
    int[] indexToRecordIdMapping = new int[numRecords];

    for (int i = 0; i < numRecords; i++) {
      currentPositions[i] = i;
      indexToRecordIdMapping[i] = i;
    }
    for (int i = 0; i < numRecords; i++) {
      int thisRecordId = indexToRecordIdMapping[i];
      int thisRecordIdPos = currentPositions[thisRecordId];

      int thatRecordId = sortedDocIds[i];
      int thatRecordIdPos = currentPositions[thatRecordId];

      // Swap the buffers
      long thisOffset = thisRecordIdPos *  recordSizeInBytes;
      long thatOffset = thatRecordIdPos *  recordSizeInBytes;

      mappedByteBuffer.copyTo(thisOffset, dimMetLbuf1, 0, recordSizeInBytes);
      mappedByteBuffer.copyTo(thatOffset, dimMetLbuf2, 0, recordSizeInBytes);

      dimMetLbuf1.copyTo(0, mappedByteBuffer, thatOffset, recordSizeInBytes);
      dimMetLbuf2.copyTo(0, mappedByteBuffer, thisOffset, recordSizeInBytes);

      indexToRecordIdMapping[i] = thatRecordId;
      indexToRecordIdMapping[thatRecordIdPos] = thisRecordId;

      currentPositions[thatRecordId] = i;
      currentPositions[thisRecordId] = thatRecordIdPos;
    }

    if (mappedByteBuffer != null) {
      mappedByteBuffer.flush();
      mappedByteBuffer.close();
    }
  }

  /**
   *
   * @param startDocId inclusive
   * @param endDocId exclusive
   * @param colIndex
   * @return start,end for each value. inclusive start, exclusive end
   */
  public Int2ObjectMap<IntPair> groupByIntColumnCount(int startDocId, int endDocId, Integer colIndex) {
    MMapBuffer mappedByteBuffer = null;
    try {
      int length = endDocId - startDocId;
      Int2ObjectMap<IntPair> rangeMap = new Int2ObjectLinkedOpenHashMap<>();
      final long startOffset = startDocId * (long) totalSizeInBytes;
      mappedByteBuffer = new MMapBuffer(file, startOffset, length * (long) totalSizeInBytes, MMapMode.READ_WRITE);
      int prevValue = -1;
      int prevStart = 0;

      for (int i = 0; i < length; i++) {
        int value = flipEndiannessIfNeeded(
            mappedByteBuffer.getInt((i * (long) totalSizeInBytes) + (colIndex * V1Constants.Numbers.INTEGER_SIZE)));

        if (prevValue != -1 && prevValue != value) {
          rangeMap.put(prevValue, new IntPair(startDocId + prevStart, startDocId + i));
          prevStart = i;
        }
        prevValue = value;
      }
      rangeMap.put(prevValue, new IntPair(startDocId + prevStart, endDocId));
      return rangeMap;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (mappedByteBuffer != null) {
        try {
          mappedByteBuffer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return EMPTY_INT_OBJECT_MAP;
  }

  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId) throws IOException {
    final int length = endDocId - startDocId;
    final long startOffset = startDocId * (long) totalSizeInBytes;
    final MMapBuffer mappedByteBuffer = new MMapBuffer(file, startOffset, length * (long) totalSizeInBytes, MMapMode.READ_WRITE);
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

        mappedByteBuffer.toDirectByteBuffer(pointer * (long) totalSizeInBytes, dimensionSizeInBytes).get(dimBuff);
        if (metricSizeInBytes > 0) {
          mappedByteBuffer.toDirectByteBuffer(pointer * (long) totalSizeInBytes + dimensionSizeInBytes,
              metricSizeInBytes).get(metBuff);
        }
        pointer = pointer + 1;
        if(pointer == length){
          try {
            mappedByteBuffer.close();
          } catch (IOException e) {
            LOGGER.error("Exception caught in record iterator", e);
          }
        }
        return Pair.of(dimBuff, metBuff);
      }
    };
  }

  /**
   * Flip the endianness of an int if needed. This is required when a file was written using
   * FileOutputStream (which is BIG_ENDIAN), but memory mapped using MMapBuffer, which uses Java Unsafe,
   * that can be LITTLE_ENDIAN if the host is LITTLE_ENDIAN.
   *
   * @param value Input integer
   * @return Flipped integer
   */
  protected static int flipEndiannessIfNeeded(int value) {
    if (nativeByteOrder == ByteOrder.LITTLE_ENDIAN) {
      return ((value & 0xff) << 24) | ((value & 0xff00) << 8) | ((value & 0xff0000) >> 8) | ((value >> 24) & 0xff);
    } else {
      return value;
    }
  }
}
