/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Record reader for Pinot segment.
 */
public class PinotSegmentRecordReader implements RecordReader {
  private final IndexSegmentImpl _indexSegment;
  private final int _numDocs;
  private final Schema _schema;
  private final Map<String, PinotSegmentColumnReader> _columnReaderMap;

  private int _nextDocId = 0;

  /**
   * Read records using the segment schema.
   */
  public PinotSegmentRecordReader(File indexDir) throws Exception {
    _indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(indexDir, ReadMode.mmap);
    try {
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) _indexSegment.getSegmentMetadata();
      _numDocs = segmentMetadata.getTotalRawDocs();
      _schema = segmentMetadata.getSchema();
      Collection<String> columnNames = _schema.getColumnNames();
      _columnReaderMap = new HashMap<>(columnNames.size());
      for (String columnName : columnNames) {
        _columnReaderMap.put(columnName, new PinotSegmentColumnReader(_indexSegment, columnName));
      }
    } catch (Exception e) {
      _indexSegment.destroy();
      throw e;
    }
  }

  /**
   * Read records using the passed in schema.
   * <p>Passed in schema must be a subset of the segment schema.
   */
  public PinotSegmentRecordReader(File indexDir, Schema schema) throws Exception {
    _indexSegment = (IndexSegmentImpl) Loaders.IndexSegment.load(indexDir, ReadMode.mmap);
    _schema = schema;
    try {
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) _indexSegment.getSegmentMetadata();
      _numDocs = segmentMetadata.getTotalRawDocs();
      Schema segmentSchema = segmentMetadata.getSchema();
      Collection<FieldSpec> fieldSpecs = _schema.getAllFieldSpecs();
      _columnReaderMap = new HashMap<>(fieldSpecs.size());
      for (FieldSpec fieldSpec : fieldSpecs) {
        String columnName = fieldSpec.getName();
        FieldSpec segmentFieldSpec = segmentSchema.getFieldSpecFor(columnName);
        Preconditions.checkState(fieldSpec.equals(segmentFieldSpec),
            "Field spec mismatch for column: %s, in the given schema: %s, in the segment schema: %s", columnName,
            fieldSpec, segmentFieldSpec);
        _columnReaderMap.put(columnName, new PinotSegmentColumnReader(_indexSegment, columnName));
      }
    } catch (Exception e) {
      _indexSegment.destroy();
      throw e;
    }
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
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String fieldName = fieldSpec.getName();
      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType()) {
          case INT:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readInt(_nextDocId));
            break;
          case LONG:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readLong(_nextDocId));
            break;
          case FLOAT:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readFloat(_nextDocId));
            break;
          case DOUBLE:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readDouble(_nextDocId));
            break;
          case STRING:
            reuse.putField(fieldName, _columnReaderMap.get(fieldName).readString(_nextDocId));
            break;
          default:
            throw new IllegalStateException(
                "Field: " + fieldName + " has illegal data type: " + fieldSpec.getDataType());
        }
      } else {
        reuse.putField(fieldName, _columnReaderMap.get(fieldName).readMV(_nextDocId));
      }
    }
    _nextDocId++;
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
    _indexSegment.destroy();
  }
}
