/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2;

import java.util.List;
import java.io.Closeable;
import java.util.Iterator;
import java.io.IOException;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import org.apache.commons.lang3.tuple.Pair;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.Pairs;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;


public class StarTreeV2DataTable implements Closeable {

  private final int _startDocId;
  private final int _dimensionSize;
  private final PinotDataBuffer _dataBuffer;

  /**
   * Constructor of the StarTreeDataTable.
   *
   * @param dataBuffer Data buffer
   * @param dimensionSize Size of all dimensions in bytes
   */
  public StarTreeV2DataTable(PinotDataBuffer dataBuffer, int dimensionSize, int starDocId) {
    Preconditions.checkState(dataBuffer.size() > 0);

    _startDocId = starDocId;
    _dataBuffer = dataBuffer;
    _dimensionSize = dimensionSize;
  }

  /**
   * Sorts the documents inside the data buffer based on the sort order.
   * <p>To reduce the number of swaps inside the data buffer, we first sort on an array which only read from the data
   * buffer, then re-arrange the actual document inside the data buffer based on the sorted array.
   * <p>This method may change the data. Close data table to flush the changes to the disk.
   *
   * @param startDocId Start document id of the range to be sorted
   * @param endDocId End document id (exclusive) of the range to be sorted
   * @param sortOrder Sort order of dimensions
   *
   * @return array of sorted doc ids.
   */
  public int[] sort(int startDocId, int endDocId, final int[] sortOrder, List<Long> docSizeIndex) {

    Preconditions.checkState(startDocId < endDocId);

    int index = 0;
    int numDocs = endDocId - startDocId;
    final int[] sortedDocIds = new int[numDocs];
    for (int i = startDocId; i < endDocId; i++) {
      sortedDocIds[index] = i;
      index++;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        long offset1 = docSizeIndex.get(sortedDocIds[i1]) - docSizeIndex.get(_startDocId);
        long offset2 = docSizeIndex.get(sortedDocIds[i2]) - docSizeIndex.get(_startDocId);
        for (int index : sortOrder) {
          int v1 = _dataBuffer.getInt(offset1 + index * Integer.BYTES);
          int v2 = _dataBuffer.getInt(offset2 + index * Integer.BYTES);
          if (v1 != v2) {
            return v1 - v2;
          }
        }
        return 0;
      }

      @Override
      public int compare(Integer o1, Integer o2) {
        throw new UnsupportedOperationException();
      }
    };

    Swapper swapper = (i, j) -> {
      int temp = sortedDocIds[i];
      sortedDocIds[i] = sortedDocIds[j];
      sortedDocIds[j] = temp;
    };
    Arrays.quickSort(0, numDocs, comparator, swapper);

    return sortedDocIds;
  }


  /**
   * Gets the iterator to iterate over the documents inside the data buffer.
   *
   * @param startDocId Start document id of the range to iterate
   * @param endDocId End document id (exclusive) of the range to iterate
   *
   * @return Iterator for pair of dimension bytes and metric bytes
   */
  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId, List<Long> docSizeIndex, int [] sortedDocsId) {
    Preconditions.checkState(startDocId < endDocId);

    return new Iterator<Pair<byte[], byte[]>>() {
      private int _currentIndex = 0;

      @Override
      public boolean hasNext() {
        return _currentIndex < sortedDocsId.length;
      }

      @Override
      public Pair<byte[], byte[]> next() {
        long totalBytes =  docSizeIndex.get(sortedDocsId[_currentIndex] + 1) - docSizeIndex.get(sortedDocsId[_currentIndex]);
        int metricSize = (int) (totalBytes - _dimensionSize);
        byte[] dimensionBytes = new byte[_dimensionSize];
        byte[] metricBytes = new byte[metricSize];
        long dimensionOffset = docSizeIndex.get(sortedDocsId[_currentIndex]) - docSizeIndex.get(_startDocId);
        _currentIndex++;
        _dataBuffer.copyTo(dimensionOffset, dimensionBytes, 0, _dimensionSize);
        _dataBuffer.copyTo(dimensionOffset + _dimensionSize, metricBytes, 0, metricSize);
        return new ImmutablePair<>(dimensionBytes, metricBytes);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
  @Override
  public void close() throws IOException {
    _dataBuffer.close();
  }

  /**
   * Groups all documents based on a dimension's value.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param dimensionId Index of the dimension to group on
   *
   * @return Map from dimension value to a pair of start docId and end docId (exclusive)
   */
  public Int2ObjectMap<Pairs.IntPair> groupOnDimension(int startDocId, int endDocId, int dimensionId, List<Long> docSizeIndex) {
    Preconditions.checkState(startDocId < endDocId);


    Int2ObjectMap<Pairs.IntPair> rangeMap = new Int2ObjectLinkedOpenHashMap<>();
    int dimensionOffset = dimensionId * Integer.BYTES;

    int currentValue = _dataBuffer.getInt(dimensionOffset);

    int groupStartDocId = startDocId;
    for (int i = startDocId; i < endDocId; i++) {
      int value = _dataBuffer.getInt( docSizeIndex.get(i) - docSizeIndex.get(_startDocId) + dimensionOffset);
      if (value != currentValue) {
        int groupEndDocId = i;
        rangeMap.put(currentValue, new Pairs.IntPair(groupStartDocId, groupEndDocId));
        currentValue = value;
        groupStartDocId = groupEndDocId;
      }
    }
    rangeMap.put(currentValue, new Pairs.IntPair(groupStartDocId, endDocId));
    return rangeMap;
  }

  /**
   * Sets the value for each document at the specified index to the specified value.
   * <p>This method may change the data. Close data table to flush the changes to the disk.
   *
   * @param dimensionId Index of the dimension to set the value
   * @param value Value to be set
   */
  public void setDimensionValue(int startDocId, int endDocId, int dimensionId, int value, List<Long>docSizeIndex) {
    for (int i = startDocId; i < endDocId; i++) {
      _dataBuffer.putInt((docSizeIndex.get(i) - docSizeIndex.get(_startDocId)) + dimensionId * Integer.BYTES, value);
    }
  }
}
