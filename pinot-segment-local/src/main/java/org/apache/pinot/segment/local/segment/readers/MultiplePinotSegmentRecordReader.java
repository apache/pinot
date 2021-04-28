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
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Record reader for multiple pinot segments.
 */
public class MultiplePinotSegmentRecordReader implements RecordReader {
  private List<PinotSegmentRecordReader> _pinotSegmentRecordReaders;
  private PriorityQueue<GenericRowWithReader> _priorityQueue;
  private Schema _schema;
  private List<String> _sortOrder;
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
   * @param schema input schema that is a subset of the segment schema
   * @param sortOrder a list of column names that represent the sorting order
   */
  public MultiplePinotSegmentRecordReader(List<File> indexDirs, @Nullable Schema schema,
      @Nullable List<String> sortOrder)
      throws Exception {
    // Initialize pinot segment record readers
    _pinotSegmentRecordReaders = new ArrayList<>(indexDirs.size());
    for (File file : indexDirs) {
      _pinotSegmentRecordReaders.add(new PinotSegmentRecordReader(file, schema, sortOrder));
    }

    // Initialize schema
    if (schema == null) {
      // Validate that segment schemas from all segments are the same if the schema is not passed.
      Set<Schema> schemas = new HashSet<>();
      for (PinotSegmentRecordReader pinotSegmentRecordReader : _pinotSegmentRecordReaders) {
        schemas.add(pinotSegmentRecordReader.getSchema());
      }
      if (schemas.size() == 1) {
        _schema = schemas.iterator().next();
      } else {
        throw new IllegalStateException("Schemas from input segments are not the same");
      }
    } else {
      // If the schema is given, the passed schema can directly be used.
      _schema = schema;
    }

    // Initialize sort order and priority queue
    _sortOrder = sortOrder;
    if (isSortedSegment()) {
      _priorityQueue = new PriorityQueue<>(_pinotSegmentRecordReaders.size());
      for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
        if (recordReader.hasNext()) {
          _priorityQueue.add(new GenericRowWithReader(recordReader.next(), recordReader, _sortOrder, _schema));
        }
      }
    }
  }

  public Schema getSchema() {
    return _schema;
  }

  /**
   * Indicate whether the segment should be sorted or not
   */
  private boolean isSortedSegment() {
    return _sortOrder != null && !_sortOrder.isEmpty();
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
  }

  @Override
  public boolean hasNext() {
    if (isSortedSegment()) {
      return _priorityQueue.size() > 0;
    } else {
      boolean hasNext = false;
      for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
        if (recordReader.hasNext()) {
          hasNext = true;
          break;
        }
      }
      return hasNext;
    }
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    if (isSortedSegment()) {
      GenericRowWithReader genericRowComparable = _priorityQueue.poll();
      GenericRow currentRow = genericRowComparable.getRow();

      // Fill reuse with the information from the currentRow
      reuse.init(currentRow);

      // If the record reader has more rows left, put back the next minimum value to the queue
      PinotSegmentRecordReader recordReader = genericRowComparable.getRecordReader();
      if (recordReader.hasNext()) {
        currentRow.clear();
        genericRowComparable.setRow(recordReader.next(currentRow));
        genericRowComparable.setRecordReader(recordReader);
        _priorityQueue.add(genericRowComparable);
      }
      return reuse;
    } else {
      // If there is no sorted column specified, simply concatenate the segments
      for (int i = 0; i < _pinotSegmentRecordReaders.size();
          i++, _currentReaderId = (_currentReaderId + 1) % _pinotSegmentRecordReaders.size()) {
        PinotSegmentRecordReader currentReader = _pinotSegmentRecordReaders.get(_currentReaderId);
        if (currentReader.hasNext()) {
          return currentReader.next(reuse);
        }
      }
      throw new RuntimeException("next is called after reading all data");
    }
  }

  @Override
  public void rewind() {
    for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
      recordReader.rewind();
    }
    // If the segment is sorted, we need to re-initialize the priority queue
    if (isSortedSegment()) {
      _priorityQueue.clear();
      for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
        if (recordReader.hasNext()) {
          _priorityQueue.add(new GenericRowWithReader(recordReader.next(), recordReader, _sortOrder, _schema));
        }
      }
    } else {
      _currentReaderId = 0;
    }
  }

  @Override
  public void close()
      throws IOException {
    for (PinotSegmentRecordReader recordReader : _pinotSegmentRecordReaders) {
      recordReader.close();
    }
  }

  /**
   * Wrapper for generic row and record reader along with sorted column. Comparison of this object is based on
   * the value of sorted column.
   *
   * TODO: add the support for multi value columns
   */
  class GenericRowWithReader implements Comparable<GenericRowWithReader> {
    private GenericRow _row;
    private PinotSegmentRecordReader _recordReader;
    private List<String> _sortOrder;
    private Schema _schema;

    public GenericRowWithReader(GenericRow row, PinotSegmentRecordReader recordReader, List<String> sortOrder,
        Schema schema) {
      _row = row;
      _recordReader = recordReader;
      _sortOrder = sortOrder;
      _schema = schema;
    }

    @Override
    public int compareTo(GenericRowWithReader o) {
      int compare = 0;
      for (String column : _sortOrder) {
        FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
        Object otherVal = o.getRow().getValue(column);
        Object thisVal = _row.getValue(column);
        if (fieldSpec.isSingleValueField()) {
          switch (fieldSpec.getDataType().getStoredType()) {
            case INT:
              compare = ((Integer) thisVal).compareTo((Integer) otherVal);
              break;
            case LONG:
              compare = ((Long) thisVal).compareTo((Long) otherVal);
              break;
            case FLOAT:
              compare = ((Float) thisVal).compareTo((Float) otherVal);
              break;
            case DOUBLE:
              compare = ((Double) thisVal).compareTo((Double) otherVal);
              break;
            case STRING:
              compare = ((String) thisVal).compareTo((String) otherVal);
              break;
            case BYTES:
              compare = ByteArray.compare((byte[]) thisVal, (byte[]) otherVal);
              break;
            default:
              throw new IllegalStateException("Unsupported column value type");
          }
        } else {
          throw new IllegalStateException("Multi value column is not supported");
        }

        if (compare != 0) {
          return compare;
        }
      }
      return compare;
    }

    public GenericRow getRow() {
      return _row;
    }

    public void setRow(GenericRow row) {
      _row = row;
    }

    public PinotSegmentRecordReader getRecordReader() {
      return _recordReader;
    }

    public void setRecordReader(PinotSegmentRecordReader recordReader) {
      _recordReader = recordReader;
    }

    public Schema getSchema() {
      return _schema;
    }

    public void setSchema(Schema schema) {
      _schema = schema;
    }
  }
}
