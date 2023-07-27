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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.sort.PinotSegmentSorter;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Record reader for Pinot segment.
 */
public class PinotSegmentRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRecordReader.class);

  private IndexSegment _indexSegment;
  private boolean _destroySegmentOnClose;
  private int _numDocs;
  private Map<String, PinotSegmentColumnReader> _columnReaderMap;
  private int[] _sortedDocIds;
  private boolean _skipDefaultNullValues;

  private int _nextDocId = 0;

  public PinotSegmentRecordReader() {
  }

  /**
   * Deprecated: use empty constructor and init() instead.
   *
   * Read records using the segment schema
   * @param indexDir input path for the segment index
   */
  @Deprecated
  public PinotSegmentRecordReader(File indexDir)
      throws Exception {
    try {
      init(indexDir, null, null, false);
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  /**
   * Deprecated: use empty constructor and init() instead.
   *
   * Read records using the segment schema with the given schema and sort order
   * <p>Passed in schema must be a subset of the segment schema.
   *
   * @param indexDir input path for the segment index
   * @param schema input schema that is a subset of the segment schema
   * @param sortOrder a list of column names that represent the sorting order
   */
  @Deprecated
  public PinotSegmentRecordReader(File indexDir, @Nullable Schema schema, @Nullable List<String> sortOrder)
      throws Exception {
    Set<String> fieldsToRead = schema != null ? schema.getPhysicalColumnNames() : null;
    try {
      init(indexDir, fieldsToRead, sortOrder, false);
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  @Override
  public void init(File indexDir, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    init(indexDir, fieldsToRead, null, true);
  }

  /**
   * Initializes the record reader from an index directory.
   *
   * @param indexDir Index directory
   * @param fieldsToRead The fields to read from the segment. If null or empty, reads all fields
   * @param sortOrder List of sorted columns
   * @param skipDefaultNullValues Whether to skip putting default null values into the record
   */
  public void init(File indexDir, @Nullable Set<String> fieldsToRead, @Nullable List<String> sortOrder,
      boolean skipDefaultNullValues) {
    IndexSegment indexSegment;
    try {
      indexSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while loading the segment from: " + indexDir, e);
    }
    init(indexSegment, true, fieldsToRead, null, sortOrder, skipDefaultNullValues);
  }

  /**
   * Initializes the record reader from a segment.
   *
   * @param indexSegment Index segment to read from
   */
  public void init(IndexSegment indexSegment) {
    init(indexSegment, false, null, null, null, false);
  }

  /**
   * Initializes the record reader from a mutable segment with optional sorted document ids.
   *
   * @param mutableSegment Mutable segment
   * @param sortedDocIds Array of sorted document ids
   */
  public void init(MutableSegment mutableSegment, @Nullable int[] sortedDocIds) {
    init(mutableSegment, false, null, sortedDocIds, null, false);
  }

  /**
   * Initializes the record reader.
   *
   * @param indexSegment Index segment to read from
   * @param destroySegmentOnClose Whether to destroy the segment when closing the record reader
   * @param fieldsToRead The fields to read from the segment. If null or empty, reads all fields
   * @param sortedDocIds Array of sorted document ids
   * @param sortOrder List of sorted columns
   * @param skipDefaultNullValues Whether to skip putting default null values into the record
   */
  private void init(IndexSegment indexSegment, boolean destroySegmentOnClose, @Nullable Set<String> fieldsToRead,
      @Nullable int[] sortedDocIds, @Nullable List<String> sortOrder, boolean skipDefaultNullValues) {
    _indexSegment = indexSegment;
    _destroySegmentOnClose = destroySegmentOnClose;
    _numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    if (_numDocs > 0) {
      _columnReaderMap = new HashMap<>();
      Set<String> columnsInSegment = _indexSegment.getPhysicalColumnNames();
      if (CollectionUtils.isEmpty(fieldsToRead)) {
        for (String column : columnsInSegment) {
          _columnReaderMap.put(column, new PinotSegmentColumnReader(indexSegment, column));
        }
      } else {
        for (String column : fieldsToRead) {
          if (columnsInSegment.contains(column)) {
            _columnReaderMap.put(column, new PinotSegmentColumnReader(indexSegment, column));
          } else {
            LOGGER.warn("Ignoring column: {} that does not exist in the segment", column);
          }
        }
      }

      if (sortedDocIds != null) {
        _sortedDocIds = sortedDocIds;
      } else {
        if (CollectionUtils.isNotEmpty(sortOrder)) {
          _sortedDocIds = new PinotSegmentSorter(_numDocs, _columnReaderMap).getSortedDocIds(sortOrder);
        } else {
          _sortedDocIds = null;
        }
      }

      _skipDefaultNullValues = skipDefaultNullValues;
    }
  }

  /**
   * Returns the sorted document ids.
   */
  @Nullable
  public int[] getSortedDocIds() {
    return _sortedDocIds;
  }

  @Override
  public boolean hasNext() {
    return _nextDocId < _numDocs;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    if (_sortedDocIds == null) {
      getRecord(_nextDocId, reuse);
    } else {
      getRecord(_sortedDocIds[_nextDocId], reuse);
    }
    _nextDocId++;
    return reuse;
  }

  public String getSegmentName() {
    return _indexSegment.getSegmentName();
  }

  public void getRecord(int docId, GenericRow buffer) {
    for (Map.Entry<String, PinotSegmentColumnReader> entry : _columnReaderMap.entrySet()) {
      String column = entry.getKey();
      PinotSegmentColumnReader columnReader = entry.getValue();
      if (!columnReader.isNull(docId)) {
        buffer.putValue(column, columnReader.getValue(docId));
      } else if (!_skipDefaultNullValues) {
        buffer.putDefaultNullValue(column, columnReader.getValue(docId));
      }
    }
  }

  public Object getValue(int docId, String column) {
    return _columnReaderMap.get(column).getValue(docId);
  }

  @Override
  public void rewind() {
    _nextDocId = 0;
  }

  @Override
  public void close()
      throws IOException {
    if (_columnReaderMap != null) {
      for (PinotSegmentColumnReader columnReader : _columnReaderMap.values()) {
        columnReader.close();
      }
    }
    if (_destroySegmentOnClose && _indexSegment != null) {
      _indexSegment.destroy();
    }
  }
}
