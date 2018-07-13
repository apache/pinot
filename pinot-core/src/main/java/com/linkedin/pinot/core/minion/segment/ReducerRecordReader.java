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
package com.linkedin.pinot.core.minion.segment;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Record reader for reducer stage of the segment conversion
 */
public class ReducerRecordReader implements RecordReader {

  private PinotSegmentRecordReader _recordReader;
  private RecordAggregator _recordAggregator;
  private List<String> _groupByColumns;

  private List<GenericRow> _rowGroup = new ArrayList<>();
  private GenericRow _nextRow = new GenericRow();
  private boolean _finished = false;
  private boolean _nextRowReturned = true;

  public ReducerRecordReader(File indexDir, RecordAggregator recordAggregator, List<String> groupByColumns)
      throws Exception {
    _recordReader = new PinotSegmentRecordReader(indexDir, null, groupByColumns);
    _recordAggregator = recordAggregator;
    _groupByColumns = groupByColumns;
  }

  @Override
  public boolean hasNext() {
    if (_finished) {
      return false;
    }

    if (!_nextRowReturned) {
      return true;
    }

    while(_recordReader.hasNext()) {
      GenericRow currentRow = _recordReader.next();

      // Grouping rows by the given group-by columns
      if (_rowGroup.isEmpty() || haveSameGroupByColumns(_rowGroup.iterator().next(), currentRow)) {
        _rowGroup.add(currentRow);
      } else {
        // Aggregate the list of rows into a single row
        _nextRow = _recordAggregator.aggregateRecords(_rowGroup);
        _rowGroup.clear();
        _rowGroup.add(currentRow);
        _nextRowReturned = false;
        return true;
      }
    }
    _finished = true;

    // Handle the last group if needed
    if (!_rowGroup.isEmpty()) {
      _nextRow = _recordAggregator.aggregateRecords(_rowGroup);
      _rowGroup.clear();
      _nextRowReturned = false;
      return true;
    }
    return false;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    Preconditions.checkState(!_nextRowReturned);
    RecordReaderUtils.copyRow(_nextRow, reuse);
    _nextRowReturned = true;
    return reuse;
  }

  @Override
  public void rewind() {
    _recordReader.rewind();
    _rowGroup.clear();
    _nextRowReturned = true;
    _finished = false;
  }

  @Override
  public Schema getSchema() {
    return _recordReader.getSchema();
  }

  @Override
  public void close() {
    _recordReader.close();
  }

  /**
   * Check that two rows are having the same dimension and time column values.
   */
  private boolean haveSameGroupByColumns(GenericRow row1, GenericRow row2) {
    for (String columnName : _groupByColumns) {
      Object value1 = row1.getValue(columnName);
      Object value2 = row2.getValue(columnName);
      if (!value1.equals(value2)) {
        return false;
      }
    }
    return true;
  }
}
