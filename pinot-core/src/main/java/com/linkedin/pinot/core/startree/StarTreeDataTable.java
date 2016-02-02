/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.linkedin.pinot.common.utils.Pairs.IntPair;


public class StarTreeDataTable {

  private File file;
  private int dimensionSizeInBytes;
  private int metricSizeInBytes;
  private int totalSizeInBytes;

  public StarTreeDataTable(File file, int dimensionSizeInBytes, int metricSizeInBytes) {
    this.file = file;
    this.dimensionSizeInBytes = dimensionSizeInBytes;
    this.metricSizeInBytes = metricSizeInBytes;
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
    RandomAccessFile randomAccessFile = null;
    try {
      int length = endRecordId - startRecordId;
      randomAccessFile = new RandomAccessFile(file, "rw");
      final int startOffset = startRecordId * totalSizeInBytes;
      final MappedByteBuffer mappedByteBuffer =
          randomAccessFile.getChannel().map(MapMode.READ_WRITE, startOffset, length * totalSizeInBytes);

      List<Integer> idList = new ArrayList<Integer>();
      for (int i = startRecordId; i < endRecordId; i++) {
        idList.add(i - startRecordId);
      }
      Comparator<Integer> comparator = new Comparator<Integer>() {
        byte[] buf1 = new byte[dimensionSizeInBytes];
        byte[] buf2 = new byte[dimensionSizeInBytes];

        @Override
        public int compare(Integer o1, Integer o2) {
          int pos1 = (o1) * totalSizeInBytes;
          int pos2 = (o2) * totalSizeInBytes;
          //System.out.println("pos1="+ pos1 +" , pos2="+ pos2);
          mappedByteBuffer.position(pos1);
          mappedByteBuffer.get(buf1);
          mappedByteBuffer.position(pos2);
          mappedByteBuffer.get(buf2);
          for (int i = startOffsetInRecord; i < endOffsetInRecord; i++) {
            int b1 = buf1[i] & 0xff;
            int b2 = buf2[i] & 0xff;
            if (b1 != b2) {
              return b1 - b2;
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
        mappedByteBuffer.position(thisRecordIdPos * totalSizeInBytes);
        mappedByteBuffer.get(buf1);
        mappedByteBuffer.position(thatRecordIdPos * totalSizeInBytes);
        mappedByteBuffer.get(buf2);

        mappedByteBuffer.position(thisRecordIdPos * totalSizeInBytes);
        mappedByteBuffer.put(buf2);
        mappedByteBuffer.position(thatRecordIdPos * totalSizeInBytes);
        mappedByteBuffer.put(buf1);
        //update the positions
        indexToRecordIdMapping[i] = thatRecordId;
        indexToRecordIdMapping[thatRecordIdPos] = thisRecordId;

        currentPositions[thatRecordId] = i;
        currentPositions[thisRecordId] = thatRecordIdPos;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(randomAccessFile);
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
    RandomAccessFile randomAccessFile = null;
    try {
      int length = endDocId - startDocId;
      Map<Integer, IntPair> rangeMap = new LinkedHashMap<>();
      randomAccessFile = new RandomAccessFile(file, "rw");
      final int startOffset = startDocId * totalSizeInBytes;
      final MappedByteBuffer mappedByteBuffer =
          randomAccessFile.getChannel().map(MapMode.READ_WRITE, startOffset, length * totalSizeInBytes);
      int prevValue = -1;
      int prevStart = 0;
      for (int i = 0; i < length; i++) {
        int value = mappedByteBuffer.getInt(i * totalSizeInBytes + colIndex * 4);
        if (prevValue != -1 && prevValue != value) {
          rangeMap.put(prevValue, new IntPair(prevStart, i));
          prevStart = i;
        }
        prevValue = value;
        if (i == length - 1) {
          rangeMap.put(prevValue, new IntPair(prevStart, length));
        }
      }
      return rangeMap;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(randomAccessFile);
    }
    return Collections.emptyMap();
  }

  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId) throws IOException {
    RandomAccessFile randomAccessFile = null;
    try {
      final int length = endDocId - startDocId;
      randomAccessFile = new RandomAccessFile(file, "rw");
      final int startOffset = startDocId * totalSizeInBytes;
      final MappedByteBuffer mappedByteBuffer =
          randomAccessFile.getChannel().map(MapMode.READ_ONLY, startOffset, length * totalSizeInBytes);
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
          mappedByteBuffer.position(pointer * totalSizeInBytes);
          mappedByteBuffer.get(dimBuff);
          if (metricSizeInBytes > 0) {
            mappedByteBuffer.get(metBuff);
          }
          pointer = pointer + 1;
          return Pair.of(dimBuff, metBuff);
        }
      };

    } catch (IOException e) {
      throw e;
    } finally {
      IOUtils.closeQuietly(randomAccessFile);
    }

  }

}
