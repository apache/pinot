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
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;


/**
 * The class <code>StarTreeDataTable</code> works on a LBufferAPI and provides the helper methods to build the
 * star-tree.
 * <p>Implemented to be able to handle the memory range greater than 2GB.
 */
public class StarTreeDataTable implements Closeable {
  private final LBufferAPI _dataBuffer;
  private final int _dimensionSize;
  private final int _metricSize;
  private final long _docSize;
  private final int _startDocId;
  private final int _endDocId;

  /**
   * Constructor of the StarTreeDataTable.
   *
   * @param dataBuffer Data buffer
   * @param dimensionSize Size of all dimensions in bytes
   * @param metricSize Size of all metrics in bytes
   * @param startDocId Start document id of the data buffer
   * @param startDocId End document id of the data buffer
   */
  public StarTreeDataTable(LBufferAPI dataBuffer, int dimensionSize, int metricSize, int startDocId, int endDocId) {
    _dataBuffer = dataBuffer;
    _dimensionSize = dimensionSize;
    _metricSize = metricSize;
    _docSize = dimensionSize + metricSize;
    _startDocId = startDocId;
    // NOTE: number of documents cannot be derived from dataBuffer.size() because for MMapBuffer, dataBuffer.size()
    // could be larger than the given length because of page alignment
    _endDocId = endDocId;
  }

  /**
   * Sort the documents inside the data buffer based on the sort order.
   * <p>To reduce the number of swaps inside the data buffer, we first sort on an array which only read from the data
   * buffer, then re-arrange the actual document inside the data buffer based on the sorted array.
   * <p>This method may change the data, call {@link #flush()} before closing the data table.
   *
   * @param startDocId Start document id of the range to be sorted
   * @param endDocId End document id (exclusive) of the range to be sorted
   * @param sortOrder Sort order of dimensions
   */
  public void sort(int startDocId, int endDocId, final int[] sortOrder) {
    // Get sorted doc ids
    int numDocs = endDocId - startDocId;
    int startDocIdOffset = startDocId - _startDocId;
    final int[] sortedDocIds = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      sortedDocIds[i] = i + startDocIdOffset;
    }

    final LBuffer dimensionBuffer1 = new LBuffer(_dimensionSize);
    final LBuffer dimensionBuffer2 = new LBuffer(_dimensionSize);

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        long offset1 = sortedDocIds[i1] * _docSize;
        long offset2 = sortedDocIds[i2] * _docSize;

        _dataBuffer.copyTo(offset1, dimensionBuffer1, 0, _dimensionSize);
        _dataBuffer.copyTo(offset2, dimensionBuffer2, 0, _dimensionSize);

        for (int index : sortOrder) {
          int v1 = dimensionBuffer1.getInt(index * V1Constants.Numbers.INTEGER_SIZE);
          int v2 = dimensionBuffer2.getInt(index * V1Constants.Numbers.INTEGER_SIZE);
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

    Swapper swapper = new Swapper() {
      @Override
      public void swap(int i, int j) {
        int temp = sortedDocIds[i];
        sortedDocIds[i] = sortedDocIds[j];
        sortedDocIds[j] = temp;
      }
    };

    try {
      Arrays.quickSort(0, numDocs, comparator, swapper);
    } finally {
      dimensionBuffer1.release();
      dimensionBuffer2.release();
    }

    // Re-arrange documents based on the sorted document ids
    // Each write places a document in it's proper location, so time complexity is O(n)
    LBuffer docBuffer = new LBuffer(_docSize);
    try {
      for (int i = 0; i < numDocs; i++) {
        int actualDocId = i + startDocIdOffset;
        if (actualDocId != sortedDocIds[i]) {
          // Copy the document at actualDocId into the first document buffer
          _dataBuffer.copyTo(actualDocId * _docSize, docBuffer, 0, _docSize);

          // The while loop will create a rotating cycle
          int currentIndex = i;
          int properDocId;
          while (actualDocId != (properDocId = sortedDocIds[currentIndex])) {
            // Put the document at properDocId into the currentDocId
            int currentDocId = currentIndex + startDocIdOffset;
            _dataBuffer.copyTo(properDocId * _docSize, _dataBuffer, currentDocId * _docSize, _docSize);
            sortedDocIds[currentIndex] = currentDocId;
            currentIndex = properDocId - startDocIdOffset;
          }

          // Put the document at actualDocId into the correct location (currentDocId)
          int currentDocId = currentIndex + startDocIdOffset;
          docBuffer.copyTo(0L, _dataBuffer, currentDocId * _docSize, _docSize);
          sortedDocIds[currentIndex] = currentDocId;
        }
      }
    } finally {
      docBuffer.release();
    }
  }

  /**
   * Group all documents based on a dimension's value.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param dimensionId Index of the dimension to group on
   * @return Map from dimension value to a pair of start docId and end docId (exclusive)
   */
  public Int2ObjectMap<IntPair> groupOnDimension(int startDocId, int endDocId, int dimensionId) {
    int startDocIdOffset = startDocId - _startDocId;
    int endDocIdOffset = endDocId - _startDocId;
    Int2ObjectMap<IntPair> rangeMap = new Int2ObjectLinkedOpenHashMap<>();
    int dimensionOffset = dimensionId * V1Constants.Numbers.INTEGER_SIZE;
    int currentValue = _dataBuffer.getInt(startDocIdOffset * _docSize + dimensionOffset);
    int groupStartDocId = startDocId;
    for (int i = startDocIdOffset + 1; i < endDocIdOffset; i++) {
      int value = _dataBuffer.getInt(i * _docSize + dimensionOffset);
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
   * Get the iterator to iterate over the documents inside the data buffer.
   *
   * @param startDocId Start document id of the range to iterate
   * @param endDocId End document id (exclusive) of the range to iterate
   * @return Iterator for pair of dimension bytes and metric bytes
   */
  public Iterator<Pair<byte[], byte[]>> iterator(int startDocId, int endDocId) {
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
        ByteBuffer byteBuffer = _dataBuffer.toDirectByteBuffer(_currentIndex++ * _docSize, (int) _docSize);
        byteBuffer.get(dimensionBytes);
        byteBuffer.get(metricBytes);
        return new ImmutablePair<>(dimensionBytes, metricBytes);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Set the value for each document at the specified index to the specified value.
   * <p>This method may change the data, call {@link #flush()} before closing the data table.
   *
   * @param dimensionId Index of the dimension to set the value
   * @param value Value to be set
   */
  public void setDimensionValue(int dimensionId, int value) {
    int numDocs = _endDocId - _startDocId;
    for (int i = 0; i < numDocs; i++) {
      _dataBuffer.putInt(i * _docSize + dimensionId * V1Constants.Numbers.INTEGER_SIZE, value);
    }
  }

  /**
   * Flush any changes made to the data buffer to the disk if necessary (no-op if data buffer is LBuffer).
   */
  public void flush() {
    if (_dataBuffer instanceof MMapBuffer) {
      ((MMapBuffer) _dataBuffer).flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (_dataBuffer instanceof MMapBuffer) {
      ((MMapBuffer) _dataBuffer).close();
    } else {
      _dataBuffer.release();
    }
  }
}
