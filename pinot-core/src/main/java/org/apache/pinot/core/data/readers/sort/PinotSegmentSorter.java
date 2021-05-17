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
package org.apache.pinot.core.data.readers.sort;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.core.data.readers.PinotSegmentColumnReader;


/**
 * Sorter implementation for pinot segments
 */
public class PinotSegmentSorter implements SegmentSorter {
  private final int _numDocs;
  private final Map<String, PinotSegmentColumnReader> _columnReaderMap;

  public PinotSegmentSorter(int numDocs, Map<String, PinotSegmentColumnReader> columnReaderMap) {
    _numDocs = numDocs;
    _columnReaderMap = columnReaderMap;
  }

  /**
   * Sort the segment by the sort order columns. Orderings are computed by comparing dictionary ids.
   *
   * TODO: add the support for no-dictionary and multi-value columns.
   *
   * @param sortOrder a list of column names that represent the sorting order
   * @return an array of sorted docIds
   */
  @Override
  public int[] getSortedDocIds(List<String> sortOrder) {
    int numSortedColumns = sortOrder.size();
    PinotSegmentColumnReader[] sortedColumnReaders = new PinotSegmentColumnReader[numSortedColumns];
    for (int i = 0; i < numSortedColumns; i++) {
      String sortedColumn = sortOrder.get(i);
      PinotSegmentColumnReader sortedColumnReader = _columnReaderMap.get(sortedColumn);
      Preconditions.checkState(sortedColumnReader != null, "Failed to find sorted column: %s", sortedColumn);
      Preconditions
          .checkState(sortedColumnReader.isSingleValue(), "Unsupported sorted multi-value column: %s", sortedColumn);
      Preconditions
          .checkState(sortedColumnReader.hasDictionary(), "Unsupported sorted no-dictionary column: %s", sortedColumn);
      sortedColumnReaders[i] = sortedColumnReader;
    }

    int[] sortedDocIds = new int[_numDocs];
    for (int i = 0; i < _numDocs; i++) {
      sortedDocIds[i] = i;
    }

    Arrays.quickSort(0, _numDocs, (i1, i2) -> {
      int docId1 = sortedDocIds[i1];
      int docId2 = sortedDocIds[i2];

      for (PinotSegmentColumnReader sortedColumnReader : sortedColumnReaders) {
        int result = sortedColumnReader.getDictionary()
            .compare(sortedColumnReader.getDictId(docId1), sortedColumnReader.getDictId(docId2));
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }, (i, j) -> {
      int temp = sortedDocIds[i];
      sortedDocIds[i] = sortedDocIds[j];
      sortedDocIds[j] = temp;
    });

    return sortedDocIds;
  }
}
