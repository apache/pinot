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
package org.apache.pinot.segment.local.segment.readers.sort;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/// Sorts documents within a segment by one or more columns. For no-dictionary columns, values are
/// pre-materialized into in-memory arrays before sorting so that the comparator never needs to
/// decompress forward-index chunks during the random-access phase of quicksort.
@SuppressWarnings({"rawtypes", "unchecked"})
public class PinotSegmentSorter implements SegmentSorter {
  private final int _numDocs;
  private final Map<String, PinotSegmentColumnReader> _columnReaderMap;

  public PinotSegmentSorter(int numDocs, Map<String, PinotSegmentColumnReader> columnReaderMap) {
    _numDocs = numDocs;
    _columnReaderMap = columnReaderMap;
  }

  @Override
  public int[] getSortedDocIds(List<String> sortOrder) {
    int numSortedColumns = sortOrder.size();
    PinotSegmentColumnReader[] sortedColumnReaders = new PinotSegmentColumnReader[numSortedColumns];
    // Pre-materialized values for no-dictionary columns; null for dictionary-encoded columns
    Comparable[][] preReadValues = new Comparable[numSortedColumns][];

    for (int i = 0; i < numSortedColumns; i++) {
      String sortedColumn = sortOrder.get(i);
      PinotSegmentColumnReader sortedColumnReader = _columnReaderMap.get(sortedColumn);
      Preconditions.checkState(sortedColumnReader != null, "Failed to find sorted column: %s", sortedColumn);
      Preconditions
          .checkState(sortedColumnReader.isSingleValue(), "Unsupported sorted multi-value column: %s", sortedColumn);
      sortedColumnReaders[i] = sortedColumnReader;

      if (!sortedColumnReader.hasDictionary()) {
        preReadValues[i] = preReadAllValues(sortedColumnReader);
      }
    }

    int[] sortedDocIds = new int[_numDocs];
    for (int i = 0; i < _numDocs; i++) {
      sortedDocIds[i] = i;
    }

    IntArrays.quickSort(sortedDocIds, (docId1, docId2) -> {
      for (int j = 0; j < numSortedColumns; j++) {
        int result;
        if (preReadValues[j] != null) {
          result = preReadValues[j][docId1].compareTo(preReadValues[j][docId2]);
        } else {
          result = sortedColumnReaders[j].compare(docId1, docId2);
        }
        if (result != 0) {
          return result;
        }
      }
      return 0;
    });
    return sortedDocIds;
  }

  /// Reads all values for a no-dictionary column into a Comparable array via a single sequential
  /// pass. This avoids repeated chunk decompression during quicksort's random-access comparisons.
  private Comparable[] preReadAllValues(PinotSegmentColumnReader reader) {
    DataType storedType = reader.getStoredType();
    Comparable[] values = new Comparable[_numDocs];
    switch (storedType) {
      case INT:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = reader.getInt(i);
        }
        break;
      case LONG:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = reader.getLong(i);
        }
        break;
      case FLOAT:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = reader.getFloat(i);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = reader.getDouble(i);
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = reader.getBigDecimal(i);
        }
        break;
      case STRING:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = reader.getString(i);
        }
        break;
      case BYTES:
        for (int i = 0; i < _numDocs; i++) {
          values[i] = new ByteArray(reader.getBytes(i));
        }
        break;
      default:
        throw new IllegalStateException("Unsupported no-dictionary column type: " + storedType);
    }
    return values;
  }

}
