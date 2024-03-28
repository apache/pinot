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
package org.apache.pinot.segment.local.segment.readers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Record reader for multiple pinot segments.
 */
public class MultiplePinotSegmentRecordReader implements RecordReader {
  private final List<PinotSegmentRecordReader> _recordReaders;
  private final List<String> _sortOrder;
  private final PriorityQueue<GenericRowWithReader> _priorityQueue;

  private int _currentReaderId;

  /**
   * Read records using the passed in schema from multiple pinot segments.
   * <p>Passed in schema must be a subset of the segment schema.
   *
   * @param indexDirs a list of input segment directory paths
   */
  public MultiplePinotSegmentRecordReader(List<File> indexDirs)
      throws Exception {
    this(indexDirs, null, null);
  }

  /**
   * Read records using the passed in schema and in the order of sorted column from multiple pinot segments.
   * <p>Passed in schema must be a subset of the segment schema.
   * <p>If sort order is not specified, it will not attempt to sort the segments and simply concatenate the records from
   * muiltiple segments.
   *
   * @param indexDirs a list of input paths for the segment indices
   * @param fieldsToRead if null or empty, reads all fields
   * @param sortOrder a list of column names that represent the sorting order
   */
  public MultiplePinotSegmentRecordReader(List<File> indexDirs, @Nullable Set<String> fieldsToRead,
      @Nullable List<String> sortOrder)
      throws Exception {
    // Initialize pinot segment record readers
    int numSegments = indexDirs.size();
    _recordReaders = new ArrayList<>(numSegments);
    for (File indexDir : indexDirs) {
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      recordReader.init(indexDir, fieldsToRead, sortOrder, false);
      _recordReaders.add(recordReader);
    }

    // Initialize sort order and priority queue
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      _sortOrder = sortOrder;
      _priorityQueue = new PriorityQueue<>(numSegments);
      for (PinotSegmentRecordReader recordReader : _recordReaders) {
        if (recordReader.hasNext()) {
          _priorityQueue.add(new GenericRowWithReader(recordReader.next(), recordReader));
        }
      }
    } else {
      _sortOrder = null;
      _priorityQueue = null;
    }
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
  }

  @Override
  public boolean hasNext() {
    if (_sortOrder != null) {
      return !_priorityQueue.isEmpty();
    } else {
      for (PinotSegmentRecordReader recordReader : _recordReaders) {
        if (recordReader.hasNext()) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    if (_sortOrder != null) {
      GenericRowWithReader genericRowComparable = _priorityQueue.poll();
      assert genericRowComparable != null;
      GenericRow currentRow = genericRowComparable.getRow();

      // Fill reuse with the information from the currentRow
      reuse.init(currentRow);

      // If the record reader has more rows left, put back the next minimum value to the queue
      PinotSegmentRecordReader recordReader = genericRowComparable.getRecordReader();
      if (recordReader.hasNext()) {
        currentRow.clear();
        _priorityQueue.add(new GenericRowWithReader(recordReader.next(currentRow), recordReader));
      }
      return reuse;
    } else {
      // If there is no sorted column specified, simply concatenate the segments
      int numSegments = _recordReaders.size();
      for (int i = 0; i < numSegments; i++, _currentReaderId = (_currentReaderId + 1) % numSegments) {
        PinotSegmentRecordReader currentReader = _recordReaders.get(_currentReaderId);
        if (currentReader.hasNext()) {
          return currentReader.next(reuse);
        }
      }
      throw new RuntimeException("next is called after reading all data");
    }
  }

  @Override
  public void rewind() {
    for (PinotSegmentRecordReader recordReader : _recordReaders) {
      recordReader.rewind();
    }
    // If the segment is sorted, we need to re-initialize the priority queue
    if (_sortOrder != null) {
      _priorityQueue.clear();
      for (PinotSegmentRecordReader recordReader : _recordReaders) {
        if (recordReader.hasNext()) {
          _priorityQueue.add(new GenericRowWithReader(recordReader.next(), recordReader));
        }
      }
    } else {
      _currentReaderId = 0;
    }
  }

  @Override
  public void close()
      throws IOException {
    for (PinotSegmentRecordReader recordReader : _recordReaders) {
      recordReader.close();
    }
  }

  /**
   * Wrapper for generic row and record reader along with sorted column. Comparison of this object is based on
   * the value of sorted column.
   *
   * TODO: add the support for multi value columns
   */
  private class GenericRowWithReader implements Comparable<GenericRowWithReader> {
    private final GenericRow _row;
    private final PinotSegmentRecordReader _recordReader;

    GenericRowWithReader(GenericRow row, PinotSegmentRecordReader recordReader) {
      _row = row;
      _recordReader = recordReader;
    }

    @SuppressWarnings({"unchecked", "rawtypes", "ConstantConditions", "NullableProblems"})
    @Override
    public int compareTo(GenericRowWithReader other) {
      for (String column : _sortOrder) {
        Object thisVal = _row.getValue(column);
        Object otherVal = other.getRow().getValue(column);
        int result;
        if (thisVal instanceof byte[]) {
          result = ByteArray.compare((byte[]) thisVal, (byte[]) otherVal);
        } else {
          result = ((Comparable) thisVal).compareTo(otherVal);
        }
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }

    public GenericRow getRow() {
      return _row;
    }

    public PinotSegmentRecordReader getRecordReader() {
      return _recordReader;
    }
  }
}
