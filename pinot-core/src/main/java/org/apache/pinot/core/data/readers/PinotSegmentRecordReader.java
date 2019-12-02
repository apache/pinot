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
package org.apache.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.readers.sort.PinotSegmentSorter;
import org.apache.pinot.core.data.readers.sort.SegmentSorter;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for Pinot segment.
 */
public class PinotSegmentRecordReader implements RecordReader {
  private final ImmutableSegment _immutableSegment;
  private final int _numDocs;
  private final Schema _schema;
  private final Map<String, PinotSegmentColumnReader> _columnReaderMap;

  private int _nextDocId = 0;
  private int[] _docIdsInSortedColumnOrder;

  /**
   * Read records using the segment schema
   * @param indexDir input path for the segment index
   */
  public PinotSegmentRecordReader(@Nonnull File indexDir)
      throws Exception {
    this(indexDir, null, null);
  }

  /**
   * Read records using the segment schema with the given schema and sort order
   * <p>Passed in schema must be a subset of the segment schema.
   *
   * @param indexDir input path for the segment index
   * @param schema input schema that is a subset of the segment schema
   * @param sortOrder a list of column names that represent the sorting order
   */
  public PinotSegmentRecordReader(@Nonnull File indexDir, @Nullable Schema schema, @Nullable List<String> sortOrder)
      throws Exception {
    _immutableSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      SegmentMetadata segmentMetadata = _immutableSegment.getSegmentMetadata();
      _numDocs = segmentMetadata.getTotalRawDocs();
      if (schema == null) {
        // In order not to expose virtual columns to client, schema shouldn't be fetched from segmentMetadata;
        // otherwise the original metadata will be modified. Hence, initialize a new schema.
        _schema = new SegmentMetadataImpl(indexDir).getSchema();
        Collection<String> columnNames = _schema.getColumnNames();
        _columnReaderMap = new HashMap<>(columnNames.size());
        for (String columnName : columnNames) {
          _columnReaderMap.put(columnName, new PinotSegmentColumnReader(_immutableSegment, columnName));
        }
      } else {
        _schema = schema;
        Schema segmentSchema = segmentMetadata.getSchema();
        Collection<FieldSpec> fieldSpecs = _schema.getAllFieldSpecs();
        _columnReaderMap = new HashMap<>(fieldSpecs.size());
        for (FieldSpec fieldSpec : fieldSpecs) {
          String columnName = fieldSpec.getName();
          FieldSpec segmentFieldSpec = segmentSchema.getFieldSpecFor(columnName);
          Preconditions.checkState(fieldSpec.equals(segmentFieldSpec),
              "Field spec mismatch for column: %s, in the given schema: %s, in the segment schema: %s", columnName,
              fieldSpec, segmentFieldSpec);
          _columnReaderMap.put(columnName, new PinotSegmentColumnReader(_immutableSegment, columnName));
        }
      }
      // Initialize sorted doc ids
      initializeSortedDocIds(_schema, sortOrder);
    } catch (Exception e) {
      _immutableSegment.destroy();
      throw e;
    }
  }

  /**
   * Prepare sorted docIds in order of the given sort order columns
   */
  private void initializeSortedDocIds(Schema schema, List<String> sortOrder) {
    if (sortOrder != null && !sortOrder.isEmpty()) {
      SegmentSorter sorter = new PinotSegmentSorter(_numDocs, schema, _columnReaderMap);
      _docIdsInSortedColumnOrder = sorter.getSortedDocIds(sortOrder);
    }
  }

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig) {
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
    if (_docIdsInSortedColumnOrder == null) {
      reuse = getRecord(reuse, _nextDocId);
    } else {
      reuse = getRecord(reuse, _docIdsInSortedColumnOrder[_nextDocId]);
    }
    _nextDocId++;
    return reuse;
  }

  /**
   * Return the row given a docId
   */
  private GenericRow getRecord(GenericRow reuse, int docId) {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      if (fieldSpec.isSingleValueField()) {
        reuse.putField(fieldName, _columnReaderMap.get(fieldName).readSV(docId, fieldSpec.getDataType()));
      } else {
        reuse.putField(fieldName, _columnReaderMap.get(fieldName).readMV(docId));
      }
    }
    return reuse;
  }

  @Override
  public void rewind() {
    _nextDocId = 0;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close() {
    _immutableSegment.destroy();
  }
}
