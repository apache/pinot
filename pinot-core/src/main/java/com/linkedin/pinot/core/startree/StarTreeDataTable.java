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
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * The StarTreeDataTable should be able to handle the memory range greater than 2GB.
 * As a result, all fields related to memory position should be declared as long to avoid int overflow.
 */
public class StarTreeDataTable {

  private File file;
  private int dimensionSizeInBytes;
  private int metricSizeInBytes;
  private int totalSizeInBytes;
  final int[] sortOrder;

  public StarTreeDataTable(File file, int dimensionSizeInBytes, int metricSizeInBytes, int[] sortOrder) {
    this.file = file;
    this.dimensionSizeInBytes = dimensionSizeInBytes;
    this.metricSizeInBytes = metricSizeInBytes;
    this.sortOrder = sortOrder;
    this.totalSizeInBytes = dimensionSizeInBytes + metricSizeInBytes;
  }

  /**
   *
   * @param startRecordId inclusive
   * @param endRecordId exclusive
   */
  public void sort(int startRecordId, int endRecordId) {
    sort(startRecordId, endRecordId, 0, dimensionSizeInBytes);
  }

  public void sort(int startRecordId, int endRecordId, final int startOffsetInRecord, final int endOffsetInRecord) {
    final MMapBuffer mappedByteBuffer;
    try {
      int length = endRecordId - startRecordId;
      final long startOffset = startRecordId * (long) totalSizeInBytes;
      mappedByteBuffer = new MMapBuffer(file, startOffset, length * (long) totalSizeInBytes, MMapMode.READ_WRITE);

      List<Integer> idList = new ArrayList<Integer>();
      for (int i = startRecordId; i < endRecordId; i++) {
        idList.add(i - startRecordId);
      }
      Comparator<Integer> comparator = new Comparator<Integer>() {
        byte[] buf1 = new byte[dimensionSizeInBytes];
        byte[] buf2 = new byte[dimensionSizeInBytes];

        @Override
        public int compare(Integer o1, Integer o2) {
          long pos1 = (o1) * (long) totalSizeInBytes;
          long pos2 = (o2) * (long) totalSizeInBytes;

          //System.out.println("pos1="+ pos1 +" , pos2="+ pos2);
          mappedByteBuffer.toDirectByteBuffer(pos1, dimensionSizeInBytes).get(buf1);
          mappedByteBuffer.toDirectByteBuffer(pos2, dimensionSizeInBytes).get(buf2);
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
      //System.out.println("AFter sorting:" + idList);
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

        //swap the buffers
        mappedByteBuffer.toDirectByteBuffer(thisRecordIdPos * (long) totalSizeInBytes, totalSizeInBytes).get(buf1);
        mappedByteBuffer.toDirectByteBuffer(thatRecordIdPos * (long) totalSizeInBytes, totalSizeInBytes).get(buf2);
        //        mappedByteBuffer.position(thisRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.get(buf1);
        //        mappedByteBuffer.position(thatRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.get(buf2);
        mappedByteBuffer.readFrom(buf2, 0, thisRecordIdPos * (long) totalSizeInBytes, totalSizeInBytes);
        mappedByteBuffer.readFrom(buf1, 0, thatRecordIdPos * (long) totalSizeInBytes, totalSizeInBytes);
        //        mappedByteBuffer.position(thisRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.put(buf2);
        //        mappedByteBuffer.position(thatRecordIdPos * totalSizeInBytes);
        //        mappedByteBuffer.put(buf1);
        //update the positions
        indexToRecordIdMapping[i] = thatRecordId;
        indexToRecordIdMapping[thatRecordIdPos] = thisRecordId;

        currentPositions[thatRecordId] = i;
        currentPositions[thisRecordId] = thatRecordIdPos;
      }
      if (mappedByteBuffer != null) {
        mappedByteBuffer.flush();
        mappedByteBuffer.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      //      IOUtils.closeQuietly(randomAccessFile);
    }
  }

  /**
   *
   * @param startDocId inclusive
   * @param endDocId exclusive
   * @param colIndex
   * @return start,end for each value. inclusive start, exclusive end
   */
  public Map<Integer, IntPair> groupByIntColumnCount(int startDocId, int endDocId, Integer colIndex) {
    MMapBuffer mappedByteBuffer = null;
    try {
      int length = endDocId - startDocId;
      Map<Integer, IntPair> rangeMap = new LinkedHashMap<>();
      final long startOffset = startDocId * (long) totalSizeInBytes;
      mappedByteBuffer = new MMapBuffer(file, startOffset, length * (long) totalSizeInBytes, MMapMode.READ_WRITE);
      int prevValue = -1;
      int prevStart = 0;
      byte[] dimBuff = new byte[dimensionSizeInBytes];
      for (int i = 0; i < length; i++) {
        mappedByteBuffer.toDirectByteBuffer(i * (long) totalSizeInBytes, dimensionSizeInBytes).get(dimBuff);
        int value = ByteBuffer.wrap(dimBuff).asIntBuffer().get(colIndex);
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
    return Collections.emptyMap();
  }

  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId) throws IOException {
    try {
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
//        mappedByteBuffer.position(pointer * totalSizeInBytes);
//        mappedByteBuffer.get(dimBuff);
          if (metricSizeInBytes > 0) {
            mappedByteBuffer.toDirectByteBuffer(pointer * (long) totalSizeInBytes + dimensionSizeInBytes,
                metricSizeInBytes).get(metBuff);
//          mappedByteBuffer.get(metBuff);
          }
          pointer = pointer + 1;
          if(pointer == length){
            try {
              mappedByteBuffer.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          return Pair.of(dimBuff, metBuff);
        }
      };

    } catch (IOException e) {
      throw e;
    } finally {
      //IOUtils.closeQuietly(randomAccessFile);
    }
  }
}
