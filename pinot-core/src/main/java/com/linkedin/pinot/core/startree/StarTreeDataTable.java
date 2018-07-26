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
package com.linkedin.pinot.core.startree;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * The class <code>StarTreeDataTable</code> provides the helper methods to build the star-tree.
 */
public class StarTreeDataTable implements Closeable {
  private final PinotDataBuffer _dataBuffer;
  private final int _dimensionSize;
  private final int _metricSize;
  private final int _docSize;
  private final long _docSizeLong;
  private final int _startDocId;

  /**
   * Constructor of the StarTreeDataTable.
   *
   * @param dataBuffer Data buffer
   * @param dimensionSize Size of all dimensions in bytes
   * @param metricSize Size of all metrics in bytes
   * @param startDocId Start document id of the data buffer
   */
  public StarTreeDataTable(PinotDataBuffer dataBuffer, int dimensionSize, int metricSize, int startDocId) {
    Preconditions.checkState(dataBuffer.size() > 0);

    _dataBuffer = dataBuffer;
    _dimensionSize = dimensionSize;
    _metricSize = metricSize;
    _docSize = dimensionSize + metricSize;
    _docSizeLong = _docSize;
    _startDocId = startDocId;
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
   */
  public void sort(int startDocId, int endDocId, final int[] sortOrder) {
    Preconditions.checkState(startDocId < endDocId);

    // Get sorted doc ids
    int numDocs = endDocId - startDocId;
    int startDocIdOffset = startDocId - _startDocId;
    final int[] sortedDocIds = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      sortedDocIds[i] = i + startDocIdOffset;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        long offset1 = sortedDocIds[i1] * _docSizeLong;
        long offset2 = sortedDocIds[i2] * _docSizeLong;
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

    // Re-arrange documents based on the sorted document ids
    // Each write places a document in it's proper location, so time complexity is O(n)
    byte[] buffer = new byte[_docSize];
    for (int i = 0; i < numDocs; i++) {
      int actualDocId = i + startDocIdOffset;
      if (actualDocId != sortedDocIds[i]) {
        // Copy the document at actualDocId into the first document buffer
        _dataBuffer.copyTo(actualDocId * _docSizeLong, buffer, 0, _docSize);

        // The while loop will create a rotating cycle
        int currentIndex = i;
        int properDocId;
        while (actualDocId != (properDocId = sortedDocIds[currentIndex])) {
          // Put the document at properDocId into the currentDocId
          int currentDocId = currentIndex + startDocIdOffset;
          _dataBuffer.copyTo(properDocId * _docSizeLong, _dataBuffer, currentDocId * _docSizeLong, _docSizeLong);
          sortedDocIds[currentIndex] = currentDocId;
          currentIndex = properDocId - startDocIdOffset;
        }

        // Put the document at actualDocId into the correct location (currentDocId)
        int currentDocId = currentIndex + startDocIdOffset;
        _dataBuffer.readFrom(currentDocId * _docSizeLong, buffer, 0, _docSize);
        sortedDocIds[currentIndex] = currentDocId;
      }
    }
  }

  /**
   * Groups all documents based on a dimension's value.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param dimensionId Index of the dimension to group on
   * @return Map from dimension value to a pair of start docId and end docId (exclusive)
   */
  public Int2ObjectMap<IntPair> groupOnDimension(int startDocId, int endDocId, int dimensionId) {
    Preconditions.checkState(startDocId < endDocId);

    int startDocIdOffset = startDocId - _startDocId;
    int endDocIdOffset = endDocId - _startDocId;
    Int2ObjectMap<IntPair> rangeMap = new Int2ObjectLinkedOpenHashMap<>();
    int dimensionOffset = dimensionId * Integer.BYTES;
    int currentValue = _dataBuffer.getInt(startDocIdOffset * _docSizeLong + dimensionOffset);
    int groupStartDocId = startDocId;
    for (int i = startDocIdOffset + 1; i < endDocIdOffset; i++) {
      int value = _dataBuffer.getInt(i * _docSizeLong + dimensionOffset);
      if (value != currentValue) {
        int groupEndDocId = i + _startDocId;
        rangeMap.put(currentValue, new IntPair(groupStartDocId, groupEndDocId));
        currentValue = value;
        groupStartDocId = groupEndDocId;
      }
    }
    rangeMap.put(currentValue, new IntPair(groupStartDocId, endDocId));
    return rangeMap;
  }

  /**
   * Gets the iterator to iterate over the documents inside the data buffer.
   *
   * @param startDocId Start document id of the range to iterate
   * @param endDocId End document id (exclusive) of the range to iterate
   * @return Iterator for pair of dimension bytes and metric bytes
   */
  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId) {
    Preconditions.checkState(startDocId < endDocId);

    final int startDocIdOffset = startDocId - _startDocId;
    final int endDocIdOffset = endDocId - _startDocId;
    return new Iterator<Pair<byte[], byte[]>>() {
      private int _currentIndex = startDocIdOffset;

      @Override
      public boolean hasNext() {
        return _currentIndex < endDocIdOffset;
      }

      @Override
      public Pair<byte[], byte[]> next() {
        byte[] dimensionBytes = new byte[_dimensionSize];
        byte[] metricBytes = new byte[_metricSize];
        long dimensionOffset = _currentIndex++ * _docSizeLong;
        _dataBuffer.copyTo(dimensionOffset, dimensionBytes, 0, _dimensionSize);
        _dataBuffer.copyTo(dimensionOffset + _dimensionSize, metricBytes, 0, _metricSize);
        return new ImmutablePair<>(dimensionBytes, metricBytes);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Sets the value for each document at the specified index to the specified value.
   * <p>This method may change the data. Close data table to flush the changes to the disk.
   *
   * @param dimensionId Index of the dimension to set the value
   * @param value Value to be set
   */
  public void setDimensionValue(int dimensionId, int value) {
    int numDocs = (int) (_dataBuffer.size() / _docSizeLong);
    for (int i = 0; i < numDocs; i++) {
      _dataBuffer.putInt(i * _docSizeLong + dimensionId * Integer.BYTES, value);
    }
  }

  @Override
  public void close() throws IOException {
    _dataBuffer.close();
  }
}
