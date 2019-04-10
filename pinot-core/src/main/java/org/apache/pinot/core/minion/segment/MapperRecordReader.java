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
package org.apache.pinot.core.minion.segment;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.MultiplePinotSegmentRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.data.readers.RecordReaderUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;


/**
 * Record reader for mapper stage of the segment conversion
 */
public class MapperRecordReader implements RecordReader {
  private MultiplePinotSegmentRecordReader _recordReader;
  private RecordTransformer _recordTransformer;
  private RecordPartitioner _recordPartitioner;
  private int _totalNumPartition;
  private int _currentPartition;

  private GenericRow _nextRow = new GenericRow();
  private boolean _finished = false;
  private boolean _nextRowReturned = true;

  public MapperRecordReader(List<File> indexDirs, RecordTransformer recordTransformer,
      RecordPartitioner recordPartitioner, int totalNumPartition, int currentPartition)
      throws Exception {
    _recordReader = new MultiplePinotSegmentRecordReader(indexDirs);
    _recordPartitioner = recordPartitioner;
    _recordTransformer = recordTransformer;
    _totalNumPartition = totalNumPartition;
    _currentPartition = currentPartition;
  }

  @Override
  public void init(SegmentGeneratorConfig segmentGeneratorConfig) {

  }

  @Override
  public boolean hasNext() {
    if (_finished) {
      return false;
    }

    if (!_nextRowReturned) {
      return true;
    }

    while (_recordReader.hasNext()) {
      _nextRow = _recordReader.next(_nextRow);
      // Filter out the records that do not belong to the current partition
      if (_recordPartitioner.getPartitionFromRecord(_nextRow, _totalNumPartition) == _currentPartition) {
        // Transform record
        _nextRow = _recordTransformer.transformRecord(_nextRow);

        // Skip the record if the row is null after transformation
        if (_nextRow == null) {
          _nextRow = new GenericRow();
        } else {
          _nextRowReturned = false;
          return true;
        }
      }
    }

    // Done with reading all records
    _finished = true;
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
    _nextRowReturned = true;
    _finished = false;
  }

  @Override
  public Schema getSchema() {
    return _recordReader.getSchema();
  }

  @Override
  public void close()
      throws IOException {
    _recordReader.close();
  }
}
