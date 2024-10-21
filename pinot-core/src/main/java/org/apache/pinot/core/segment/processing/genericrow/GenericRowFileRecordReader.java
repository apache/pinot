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
package org.apache.pinot.core.segment.processing.genericrow;

import it.unimi.dsi.fastutil.Arrays;
import java.io.File;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for the GenericRow file.
 */
public class GenericRowFileRecordReader implements RecordReader {
  private final GenericRowFileReader _fileReader;
  private final int _startRowId;
  private final int _endRowId;
  private final int[] _sortedRowIds;

  private int _nextRowId;

  public GenericRowFileRecordReader(GenericRowFileReader fileReader) {
    _fileReader = fileReader;
    int numRows = fileReader.getNumRows();
    _startRowId = 0;
    _endRowId = numRows;
    if (fileReader.getNumSortFields() > 0) {
      _sortedRowIds = new int[numRows];
      for (int i = 0; i < numRows; i++) {
        _sortedRowIds[i] = i;
      }
      Arrays
          .quickSort(0, _endRowId, (i1, i2) -> _fileReader.compare(_sortedRowIds[i1], _sortedRowIds[i2]), (i1, i2) -> {
            int temp = _sortedRowIds[i1];
            _sortedRowIds[i1] = _sortedRowIds[i2];
            _sortedRowIds[i2] = temp;
          });
    } else {
      _sortedRowIds = null;
    }
  }

  private GenericRowFileRecordReader(GenericRowFileReader fileReader, int startRowId, int endRowId,
      @Nullable int[] sortedRowIds) {
    _fileReader = fileReader;
    _startRowId = startRowId;
    _endRowId = endRowId;
    _sortedRowIds = sortedRowIds;

    _nextRowId = startRowId;
  }

  /**
   * Returns a record reader for the given row id range.
   */
  public GenericRowFileRecordReader getRecordReaderForRange(int startRowId, int endRowId) {
    return new GenericRowFileRecordReader(_fileReader, startRowId, endRowId, _sortedRowIds);
  }

  /**
   * Reads the data of the given row id into the given buffer row.
   */
  public void read(int rowId, GenericRow buffer) {
    if (_sortedRowIds != null) {
      rowId = _sortedRowIds[rowId];
    }
    _fileReader.read(rowId, buffer);
  }

  /**
   * Compares the records at the given row ids.
   */
  public int compare(int rowId1, int rowId2) {
    assert _sortedRowIds != null;
    return _fileReader.compare(_sortedRowIds[rowId1], _sortedRowIds[rowId2]);
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    return _nextRowId < _endRowId;
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    int rowId = _sortedRowIds != null ? _sortedRowIds[_nextRowId++] : _nextRowId++;
    _fileReader.read(rowId, reuse);
    return reuse;
  }

  @Override
  public void rewind() {
    _nextRowId = _startRowId;
  }

  @Override
  public void close() {
  }
}
