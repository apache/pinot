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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator extends BaseSegmentCreator {
  private int _docIdCounter;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      TreeMap<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir,
      @Nullable int[] immutableToMutableIdMap, @Nullable InstanceType instanceType)
      throws Exception {
    _docIdCounter = 0;
    Map<String, ColumnIndexCreators> colIndexes = Maps.newHashMapWithExpectedSize(
        segmentCreationSpec.getIndexConfigsByColName().size());
    initializeCommon(segmentCreationSpec, segmentIndexCreationInfo, indexCreationInfoMap,
        schema, outDir, colIndexes, immutableToMutableIdMap, instanceType);
  }

  /**
   * @deprecated use {@link ForwardIndexType#getDefaultCompressionType(FieldType)} instead
   */
  @Deprecated
  public static ChunkCompressionType getDefaultCompressionType(FieldType fieldType) {
    return ForwardIndexType.getDefaultCompressionType(fieldType);
  }

  @Override
  public void indexRow(GenericRow row)
      throws IOException {
    for (Map.Entry<String, ColumnIndexCreators> byColEntry : _colIndexes.entrySet()) {
      String columnName = byColEntry.getKey();
      ColumnIndexCreators colIndexes = byColEntry.getValue();

      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      List<IndexCreator> creatorsByIndex = colIndexes.getIndexCreators();

      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
      SegmentDictionaryCreator dictionaryCreator = colIndexes.getDictionaryCreator();
      try {
        if (fieldSpec.isSingleValueField()) {
          indexSingleValueRow(dictionaryCreator, columnValueToIndex, creatorsByIndex);
        } else {
          indexMultiValueRow(dictionaryCreator, (Object[]) columnValueToIndex, creatorsByIndex);
        }
      } catch (JsonParseException jpe) {
        throw new ColumnJsonParserException(columnName, jpe);
      }

      // If row has null value for given column name, add to null value vector
      if (colIndexes.getNullValueVectorCreator() != null) {
        if (row.isNullValue(columnName)) {
          colIndexes.getNullValueVectorCreator().setNull(_docIdCounter);
        }
      }
    }

    _docIdCounter++;
  }

  /**
   * Indexes a column from the given segment.
   *
   * @param columnName The name of the column to index
   * @param sortedDocIds If not null, provides the sorted order of documents for processing
   * @param segment The segment containing the column data
   */
  @Override
  public void indexColumn(String columnName, @Nullable int[] sortedDocIds, IndexSegment segment)
      throws IOException {
    indexColumn(columnName, sortedDocIds, segment, null);
  }

  /**
   * Indexes a column from the given segment.
   *
   * @param columnName The name of the column to index
   * @param sortedDocIds If not null, provides the sorted order of documents for processing
   * @param segment The segment containing the column data
   * @param validDocIds If not null, only processes documents that are marked as valid in this bitmap.
   *                    When null, all documents in the segment are processed. This is used for
   *                    commit-time compaction to skip invalid/deleted documents during indexing.
   */
  @Override
  public void indexColumn(String columnName, @Nullable int[] sortedDocIds, IndexSegment segment,
      @Nullable ThreadSafeMutableRoaringBitmap validDocIds)
      throws IOException {
    // Iterate over each value in the column
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
    if (numDocs == 0) {
      return;
    }
    try (PinotSegmentColumnReader colReader = new PinotSegmentColumnReader(segment, columnName)) {
      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
      ColumnIndexCreators colIndexes = _colIndexes.get(columnName);
      if (sortedDocIds != null) {
        int onDiskDocId = 0;
        for (int docId : sortedDocIds) {
          // If validDodIds are provided, only index column if it's a valid doc
          if (validDocIds == null || validDocIds.contains(docId)) {
            indexColumnValue(colReader, columnName, fieldSpec, colIndexes, docId, onDiskDocId);
            onDiskDocId++;
          }
        }
      } else {
        int onDiskDocId = 0;
        for (int docId = 0; docId < numDocs; docId++) {
          // If validDodIds are provided, only index column if it's a valid doc
          if (validDocIds == null || validDocIds.contains(docId)) {
            indexColumnValue(colReader, columnName, fieldSpec, colIndexes, docId, onDiskDocId);
            onDiskDocId++;
          }
        }
      }
    }
  }

  /**
   * Index a column using a ColumnReader (column-major approach).
   * This method processes the column values using the iterator pattern from ColumnReader.
   *
   * @param columnName Name of the column to index
   * @param columnReader ColumnReader for the column data
   * @throws IOException if indexing fails
   */
  @Override
  public void indexColumn(String columnName, ColumnReader columnReader)
      throws IOException {
    List<IndexCreator> creatorsByIndex = _colIndexes.get(columnName).getIndexCreators();
    NullValueVectorCreator nullVec = _colIndexes.get(columnName).getNullValueVectorCreator();
    FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
    SegmentDictionaryCreator dictionaryCreator = _colIndexes.get(columnName).getDictionaryCreator();

    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    Object reuseColumnValueToIndex;

    // Reset column reader to start from beginning
    columnReader.rewind();

    int docId = 0;
    while (columnReader.hasNext()) {
      reuseColumnValueToIndex = columnReader.next();

      // Handle null values
      if (reuseColumnValueToIndex == null) {
        if (nullVec != null) {
          nullVec.setNull(docId);
        }
        reuseColumnValueToIndex = defaultNullValue;
      }

      try {
        if (fieldSpec.isSingleValueField()) {
          indexSingleValueRow(dictionaryCreator, reuseColumnValueToIndex, creatorsByIndex);
        } else {
          indexMultiValueRow(dictionaryCreator, (Object[]) reuseColumnValueToIndex, creatorsByIndex);
        }
      } catch (JsonParseException jpe) {
        throw new ColumnJsonParserException(columnName, jpe);
      }

      docId++;
    }
  }

  private void indexColumnValue(PinotSegmentColumnReader colReader,
      String columnName, FieldSpec fieldSpec,
      ColumnIndexCreators colIndexes, int sourceDocId, int onDiskDocPos)
      throws IOException {
    List<IndexCreator> creatorsByIndex = colIndexes.getIndexCreators();
    NullValueVectorCreator nullVec = colIndexes.getNullValueVectorCreator();
    SegmentDictionaryCreator dictionaryCreator = colIndexes.getDictionaryCreator();
    Object columnValueToIndex = colReader.getValue(sourceDocId);
    if (columnValueToIndex == null) {
      throw new RuntimeException("Null value for column:" + columnName);
    }

    if (fieldSpec.isSingleValueField()) {
      indexSingleValueRow(dictionaryCreator, columnValueToIndex, creatorsByIndex);
    } else {
      indexMultiValueRow(dictionaryCreator, (Object[]) columnValueToIndex, creatorsByIndex);
    }

    if (nullVec != null) {
      if (colReader.isNull(sourceDocId)) {
        nullVec.setNull(onDiskDocPos);
      }
    }
  }

  private void indexSingleValueRow(SegmentDictionaryCreator dictionaryCreator, Object value,
      List<IndexCreator> creatorsByIndex)
      throws IOException {
    int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
    for (IndexCreator creator : creatorsByIndex) {
      creator.add(value, dictId);
    }
  }

  private void indexMultiValueRow(SegmentDictionaryCreator dictionaryCreator, Object[] values,
      List<IndexCreator> creatorsByIndex)
      throws IOException {
    int[] dictId = dictionaryCreator != null ? dictionaryCreator.indexOfMV(values) : null;
    for (IndexCreator creator : creatorsByIndex) {
      creator.add(values, dictId);
    }
  }
}
