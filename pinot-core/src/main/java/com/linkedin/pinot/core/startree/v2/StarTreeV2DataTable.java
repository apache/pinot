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

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.io.Closeable;
import java.io.IOException;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


public class StarTreeV2DataTable implements Closeable {

  private final int _dimensionSize;
  private final PinotDataBuffer _dataBuffer;

  /**
   * Constructor of the StarTreeDataTable.
   *
   * @param dataBuffer Data buffer
   * @param dimensionSize Size of all dimensions in bytes
   */
  public StarTreeV2DataTable(PinotDataBuffer dataBuffer, int dimensionSize) {
    Preconditions.checkState(dataBuffer.size() > 0);

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
   */
  public int[] sort(int startDocId, int endDocId, final int[] sortOrder, List<Long> docSizeIndex) {

    Preconditions.checkState(startDocId < endDocId);

    int numDocs = endDocId - startDocId;
    final int[] sortedDocIds = new int[numDocs];
    for (int i = startDocId; i < endDocId; i++) {
      sortedDocIds[i] = i;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        long offset1 = docSizeIndex.get(sortedDocIds[i1]);
        long offset2 = docSizeIndex.get(sortedDocIds[i2]);
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
        long dimensionOffset = docSizeIndex.get(sortedDocsId[_currentIndex]);
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
}
