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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.PinotDataType;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator extends BaseSegmentCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);

  private int _nextDocId;

  // Column-major build-path accounting, populated only by indexColumn(String, ColumnReader) and summarized
  // once via logColumnMajorBuildPathSummary(). A fresh creator is instantiated per segment build, so these
  // reset naturally; the row-major path never touches them.
  private int _columnMajorTypedFastPathColumns;
  private int _columnMajorObjectPathColumns;

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
          colIndexes.getNullValueVectorCreator().setNull(_nextDocId);
        }
      }
    }

    _nextDocId++;
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
      @Nullable RoaringBitmap validDocIds)
      throws IOException {
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
    if (numDocs == 0) {
      return;
    }

    FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);

    // OPEN_STRUCT parent columns have no forward index — their per-doc value is reconstructed as a
    // map from the per-key child columns via OpenStructDataSource.getMapValue(). Feed that map to
    // the OpenStructColumnSplitter so it can classify keys and write the child column indexes.
    if (fieldSpec.getDataType() == FieldSpec.DataType.OPEN_STRUCT) {
      DataSource dataSource = segment.getDataSourceNullable(columnName);
      if (dataSource instanceof OpenStructDataSource) {
        indexOpenStructColumn(columnName, (OpenStructDataSource) dataSource, numDocs, sortedDocIds, validDocIds);
      }
      return;
    }

    try (PinotSegmentColumnReader colReader = new PinotSegmentColumnReader(segment, columnName)) {
      ColumnIndexCreators colIndexes = _colIndexes.get(columnName);
      if (sortedDocIds != null) {
        int onDiskDocId = 0;
        for (int docId : sortedDocIds) {
          if (validDocIds == null || validDocIds.contains(docId)) {
            indexColumnValue(colReader, columnName, fieldSpec, colIndexes, docId, onDiskDocId);
            onDiskDocId++;
          }
        }
      } else {
        int onDiskDocId = 0;
        for (int docId = 0; docId < numDocs; docId++) {
          if (validDocIds == null || validDocIds.contains(docId)) {
            indexColumnValue(colReader, columnName, fieldSpec, colIndexes, docId, onDiskDocId);
            onDiskDocId++;
          }
        }
      }
    }
  }

  private void indexOpenStructColumn(String columnName, OpenStructDataSource dataSource, int numDocs,
      @Nullable int[] sortedDocIds, @Nullable RoaringBitmap validDocIds)
      throws IOException {
    List<IndexCreator> creators = _colIndexes.get(columnName).getIndexCreators();
    if (sortedDocIds != null) {
      for (int docId : sortedDocIds) {
        if (validDocIds == null || validDocIds.contains(docId)) {
          indexOpenStructDoc(dataSource, docId, creators);
        }
      }
    } else {
      for (int docId = 0; docId < numDocs; docId++) {
        if (validDocIds == null || validDocIds.contains(docId)) {
          indexOpenStructDoc(dataSource, docId, creators);
        }
      }
    }
  }

  private static void indexOpenStructDoc(OpenStructDataSource dataSource, int docId, List<IndexCreator> creators)
      throws IOException {
    Map<String, Object> value = dataSource.getMapValue(docId);
    Object toIndex = value != null ? value : Collections.emptyMap();
    for (IndexCreator creator : creators) {
      creator.add(toIndex, -1);
    }
  }

  /**
   * Index a column using a ColumnReader (column-major approach).
   * This method processes the column values by document ID using random access from ColumnReader.
   *
   * @param columnName Name of the column to index
   * @param columnReader ColumnReader for the column data
   * @throws IOException if indexing fails
   */
  @Override
  public void indexColumn(String columnName, ColumnReader columnReader)
      throws IOException {
    // The statistics pass already traversed this reader; reset it to document 0 for this index-writing pass.
    columnReader.rewind();
    List<IndexCreator> creatorsByIndex = _colIndexes.get(columnName).getIndexCreators();
    NullValueVectorCreator nullVec = _colIndexes.get(columnName).getNullValueVectorCreator();
    FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
    SegmentDictionaryCreator dictionaryCreator = _colIndexes.get(columnName).getDictionaryCreator();

    PinotDataType destDataType = PinotDataType.getPinotDataTypeForIngestion(fieldSpec);

    int numDocs = columnReader.getTotalDocs();

    // Typed fast path: for a single-value INT/LONG/FLOAT/DOUBLE column the reader can serve a primitive
    // directly (ColumnReader.getValueType()), the dictionary offers indexOfSV(primitive), and every index
    // creator offers addInt/addLong/... (ForwardIndexCreator, CombinedInvertedIndexCreator and
    // DictionaryBasedInvertedIndexCreator de-box; any other creator falls back to the boxing default).
    // Driving the column through those eliminates the per-value Integer/Long/Float/Double box that
    // getValue(docId) -> indexOfSV(Object) -> add(Object, ...) incurs. Null docs (rare) and everything else
    // (multi-value, BIG_DECIMAL/STRING/BYTES, BOOLEAN/TIMESTAMP coercion) fall through to the Object
    // path below, which stays the single source of truth for null and type handling.
    if (fieldSpec.isSingleValueField()
        && indexSingleValuePrimitive(columnName, fieldSpec, destDataType, columnReader, dictionaryCreator,
            creatorsByIndex, nullVec, numDocs)) {
      _columnMajorTypedFastPathColumns++;
      return;
    }
    // Multi-value columns, non-primitive single-value types, and fast-path primitives whose reader serves a
    // different physical type all fall through to the Object path below (the single source of truth for null
    // and type handling).
    _columnMajorObjectPathColumns++;

    Object reuseColumnValueToIndex;
    for (int docId = 0; docId < numDocs; docId++) {
      Object rawValue = columnReader.getValue(docId);

      // Record whole-value-null docs in the null-value vector BEFORE substituting the default (matching
      // the row-major path, which marks null only for whole-value nulls). The column-major driver runs
      // with no transform pipeline, so normalize the value the way the row-major NullValueTransformer +
      // DataTypeTransformer would (null -> column default, coerce to the column's stored type), shared
      // with the stats path via ColumnarValueNormalizer.
      if (rawValue == null && nullVec != null) {
        nullVec.setNull(docId);
      }
      reuseColumnValueToIndex = ColumnarValueNormalizer.normalize(columnName, fieldSpec, destDataType, rawValue);

      try {
        if (fieldSpec.isSingleValueField()) {
          indexSingleValueRow(dictionaryCreator, reuseColumnValueToIndex, creatorsByIndex);
        } else {
          indexMultiValueRow(dictionaryCreator, (Object[]) reuseColumnValueToIndex, creatorsByIndex);
        }
      } catch (JsonParseException jpe) {
        throw new ColumnJsonParserException(columnName, jpe);
      }
    }
  }

  /**
   * Typed, allocation-free index write for a single-value primitive column. Returns {@code true} if it consumed
   * the column (the reader served it as INT / LONG / FLOAT / DOUBLE directly). Otherwise it returns {@code false}
   * — without reading any value — either because the column is not a fast-path primitive, or because it is
   * one but the reader serves a different physical type, so the caller falls back to the Object path. Drives each
   * value through {@code ColumnReader.getInt(docId)}/... -> {@code indexOfSV(primitive)} -> {@code
   * IndexCreator.addInt(primitive, dictId)}, avoiding the box that {@code getValue(docId) -> indexOfSV(Object) ->
   * add(Object, ...)} incurs. Null docs (rare) are routed through the shared Object handling for exact parity,
   * including whole-value-null marking in the null-value vector.
   */
  private boolean indexSingleValuePrimitive(String columnName, FieldSpec fieldSpec, PinotDataType destDataType,
      ColumnReader columnReader, SegmentDictionaryCreator dictionaryCreator, List<IndexCreator> creatorsByIndex,
      NullValueVectorCreator nullVec, int numDocs)
      throws IOException {
    PinotDataType valueType = columnReader.getValueType();
    switch (fieldSpec.getDataType()) {
      case INT: {
        if (valueType != PinotDataType.INT) {
          return false;
        }
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.isNull(docId)) {
            indexSingleValueRow(dictionaryCreator,
                normalizeNullRow(columnName, fieldSpec, destDataType, columnReader, nullVec, docId), creatorsByIndex);
          } else {
            int value = columnReader.getInt(docId);
            int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
            for (IndexCreator creator : creatorsByIndex) {
              creator.addInt(value, dictId);
            }
          }
        }
        return true;
      }
      case LONG: {
        if (valueType != PinotDataType.LONG) {
          return false;
        }
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.isNull(docId)) {
            indexSingleValueRow(dictionaryCreator,
                normalizeNullRow(columnName, fieldSpec, destDataType, columnReader, nullVec, docId), creatorsByIndex);
          } else {
            long value = columnReader.getLong(docId);
            int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
            for (IndexCreator creator : creatorsByIndex) {
              creator.addLong(value, dictId);
            }
          }
        }
        return true;
      }
      case FLOAT: {
        if (valueType != PinotDataType.FLOAT) {
          return false;
        }
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.isNull(docId)) {
            indexSingleValueRow(dictionaryCreator,
                normalizeNullRow(columnName, fieldSpec, destDataType, columnReader, nullVec, docId), creatorsByIndex);
          } else {
            float value = columnReader.getFloat(docId);
            int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
            for (IndexCreator creator : creatorsByIndex) {
              creator.addFloat(value, dictId);
            }
          }
        }
        return true;
      }
      case DOUBLE: {
        if (valueType != PinotDataType.DOUBLE) {
          return false;
        }
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.isNull(docId)) {
            indexSingleValueRow(dictionaryCreator,
                normalizeNullRow(columnName, fieldSpec, destDataType, columnReader, nullVec, docId), creatorsByIndex);
          } else {
            double value = columnReader.getDouble(docId);
            int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
            for (IndexCreator creator : creatorsByIndex) {
              creator.addDouble(value, dictId);
            }
          }
        }
        return true;
      }
      default:
        return false;
    }
  }

  /**
   * Read a whole-value-null doc on the typed fast path and resolve it exactly as the Object path does:
   * read the raw value (an actual {@code null}, or a stored sentinel from a segment source), mark the
   * null-value vector only for an actual {@code null}, and return the normalized value (the column
   * default) for the shared single-value row helper to index.
   */
  private Object normalizeNullRow(String columnName, FieldSpec fieldSpec, PinotDataType destDataType,
      ColumnReader columnReader, NullValueVectorCreator nullVec, int docId)
      throws IOException {
    Object rawValue = columnReader.getValue(docId);
    if (rawValue == null && nullVec != null) {
      nullVec.setNull(docId);
    }
    return ColumnarValueNormalizer.normalize(columnName, fieldSpec, destDataType, rawValue);
  }

  /**
   * Emit a single INFO line, once per column-major build after all columns are indexed, reporting how many
   * single-value primitive columns took the typed allocation-free fast path versus the Object path (multi-value,
   * non-primitive single-value, or a fast-path primitive whose source type did not match the schema).
   */
  void logColumnMajorBuildPathSummary() {
    LOGGER.info("Column-major build path for segment {}: {} column(s) via typed single-value primitive fast path, "
            + "{} column(s) via Object path",
        getSegmentName(), _columnMajorTypedFastPathColumns, _columnMajorObjectPathColumns);
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
