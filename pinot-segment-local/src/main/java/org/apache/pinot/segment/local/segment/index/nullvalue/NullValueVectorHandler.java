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
package org.apache.pinot.segment.local.segment.index.nullvalue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.NullValueVectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.IS_NON_NULL;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;


/// Backfills a null value vector for opted-in columns that don't already have one, by treating every value equal to the
/// column's default null value as null.
///
/// Backfill is per-column opt-in via the null value vector index config ([NullValueVectorConfig#isBackfill], set under
/// the column's `indexes` config), because it is a lossy reconstruction: a genuine value equal to the default null
/// value is also marked null. It should be enabled only for columns whose default null value is a sentinel that does
/// not occur in the data (e.g. dimension `MIN_VALUE`), not for metric/boolean columns whose default is a common value
/// like `0`. Among opted-in columns, the handler targets those that:
/// - have null handling enabled (this is the [NullValueVectorConfig#isEnabled] state, derived from column-based
///   [org.apache.pinot.spi.data.FieldSpec#isNullable] or the table-level `nullHandlingEnabled` flag);
/// - have not already been null-handled — i.e. neither the [ColumnMetadata#isNonNull] metadata flag is
///   set nor a null value vector file exists (segments ingested before null handling was turned on);
/// - have a forward index to scan and a supported scalar stored type.
///
/// The backfill mirrors how ingestion records nulls: a null single-value entry is stored as the default null value, and
/// a null multi-value entry is stored as a single-element array holding the default null value. A doc is therefore
/// marked null when its stored single value equals the default null value, or when its multi-value entry is a
/// single-element array whose sole element equals the default null value.
///
/// A bitmap file is written only when the column has at least one null, exactly like segment creation. The no-null
/// case is recorded solely via the [V1Constants.MetadataKeys.Column#IS_NON_NULL] metadata flag, so the
/// column is not perpetually re-eligible for backfill and behaves symmetrically with segment creation.
///
/// This is a lossy reconstruction: a genuine value that happens to equal the default null value is also marked null.
/// That trade-off is accepted by opting into the feature.
@SuppressWarnings({"rawtypes"})
public class NullValueVectorHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NullValueVectorHandler.class);

  private final Set<String> _backfillColumns;

  public NullValueVectorHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    // Collect the columns whose resolved null value vector config has both null handling enabled and backfill opted
    // in. `enabled` is derived from null handling and `backfill` from the column's `indexes` config (see
    // NullValueIndexType.createDeserializer).
    _backfillColumns = new HashSet<>();
    for (Map.Entry<String, FieldIndexConfigs> entry : _fieldIndexConfigs.entrySet()) {
      NullValueVectorConfig config = entry.getValue().getConfig(StandardIndexes.nullValueVector());
      if (config.isEnabled() && config.isBackfill()) {
        _backfillColumns.add(entry.getKey());
      }
    }
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    if (_backfillColumns.isEmpty()) {
      return false;
    }
    return !getColumnsToBackfill(segmentReader).isEmpty();
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    if (_backfillColumns.isEmpty()) {
      return;
    }
    List<ColumnMetadata> columnsToBackfill = getColumnsToBackfill(segmentWriter);
    if (columnsToBackfill.isEmpty()) {
      return;
    }
    Map<String, String> metadataUpdates = new HashMap<>();
    for (ColumnMetadata columnMetadata : columnsToBackfill) {
      boolean isNonNull = backfillColumn(segmentWriter, columnMetadata);
      if (isNonNull) {
        // The column has no null values, so no bitmap file was written. Record the metadata flag so it is not
        // re-scanned on subsequent reloads, keeping the behavior symmetric with segment creation (see
        // BaseSegmentCreator). Columns that do have null values are identified by the bitmap file and need no flag.
        metadataUpdates.put(getKeyFor(columnMetadata.getColumnName(), IS_NON_NULL), String.valueOf(true));
      }
    }
    if (!metadataUpdates.isEmpty()) {
      SegmentMetadataUtils.updateMetadataProperties(_segmentDirectory, metadataUpdates);
    }
  }

  /// Returns the eligible columns among the opted-in set: the column has not already been null-handled (neither the
  /// [ColumnMetadata#isNonNull] metadata flag is set nor a null value vector file exists) and has a forward index to
  /// scan. Null handling being enabled and the stored type being backfill-supported are already guaranteed — the
  /// former when building `_backfillColumns`, the latter by [NullValueIndexType#validate] at config time.
  private List<ColumnMetadata> getColumnsToBackfill(SegmentDirectory.Reader segmentReader) {
    List<ColumnMetadata> columns = new ArrayList<>();
    for (String column : _backfillColumns) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (columnMetadata == null) {
        continue;
      }
      if (columnMetadata.isNonNull()
          || segmentReader.hasIndexFor(column, StandardIndexes.nullValueVector())) {
        // Null handling was already applied to this column: either the metadata flag is set (column-based signal,
        // covers the no-null case), or a null value vector file exists (covers older segments without the flag).
        continue;
      }
      if (!segmentReader.hasIndexFor(column, StandardIndexes.forward())) {
        // Cannot scan a forward-index-disabled column; skip it.
        continue;
      }
      columns.add(columnMetadata);
    }
    return columns;
  }

  /// Scans the column and writes its null value vector bitmap file when it has at least one null value. Returns `true`
  /// when the column has no null values (bitmap skipped), in which case the caller records the metadata flag instead.
  private boolean backfillColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    File inProgress = new File(indexDir, columnName + V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION
        + ".inprogress");
    File nullValueVectorFile = new File(indexDir, columnName + V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted. Remove the leftover null value vector if any.
      FileUtils.deleteQuietly(nullValueVectorFile);
    }

    LOGGER.info("Backfilling null value vector for segment: {}, column: {}", segmentName, columnName);
    IndexReaderFactory<ForwardIndexReader> forwardReaderFactory = StandardIndexes.forward().getReaderFactory();
    int numDocs = columnMetadata.getTotalDocs();
    RoaringBitmap nullBitmap = new RoaringBitmap();
    try (ForwardIndexReader forwardIndexReader = forwardReaderFactory.createIndexReader(segmentWriter,
        _fieldIndexConfigs.get(columnName), columnMetadata);
        Dictionary dictionary =
            columnMetadata.hasDictionary() ? DictionaryIndexType.read(segmentWriter, columnMetadata) : null;
        PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(columnName, forwardIndexReader, dictionary,
            null, columnMetadata.getMaxNumberOfMultiValues())) {
      Object defaultNullValue = columnMetadata.getFieldSpec().getDefaultNullValue();
      if (columnMetadata.isSingleValue()) {
        markNullSingleValue(columnReader, defaultNullValue, numDocs, nullBitmap);
      } else {
        markNullMultiValue(columnReader, defaultNullValue, numDocs, nullBitmap);
      }
    }

    // Only write a bitmap file when the column actually has null values, matching the segment-creation behavior
    // (NullValueVectorCreator.seal() skips empty bitmaps). The no-null case is recorded via the column metadata flag
    // set in updateIndices, so there is no need for an empty bitmap file.
    if (!nullBitmap.isEmpty()) {
      nullBitmap.runOptimize();
      try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(nullValueVectorFile))) {
        nullBitmap.serialize(outputStream);
      }
      // For v3, fold the generated file into the single index file and remove it.
      if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
        LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, nullValueVectorFile,
            StandardIndexes.nullValueVector());
      }
    }
    LOGGER.info("Backfilled null value vector for segment: {}, column: {}, numNulls: {}", segmentName, columnName,
        nullBitmap.getCardinality());

    FileUtils.deleteQuietly(inProgress);
    return nullBitmap.isEmpty();
  }

  private static void markNullSingleValue(PinotSegmentColumnReader columnReader, Object defaultNullValue, int numDocs,
      RoaringBitmap nullBitmap) {
    switch (columnReader.getValueType()) {
      case INT: {
        int nullValue = (Integer) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.getInt(docId) == nullValue) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case LONG: {
        long nullValue = (Long) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.getLong(docId) == nullValue) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case FLOAT: {
        float nullValue = (Float) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (Float.compare(columnReader.getFloat(docId), nullValue) == 0) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case DOUBLE: {
        double nullValue = (Double) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (Double.compare(columnReader.getDouble(docId), nullValue) == 0) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal nullValue = (BigDecimal) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (columnReader.getBigDecimal(docId).compareTo(nullValue) == 0) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case STRING: {
        String nullValue = (String) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (nullValue.equals(columnReader.getString(docId))) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case BYTES: {
        byte[] nullValue = (byte[]) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          if (Arrays.equals(columnReader.getBytes(docId), nullValue)) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      default:
        throw new IllegalStateException(
            "Unsupported stored type for null value vector backfill: " + columnReader.getValueType());
    }
  }

  private static void markNullMultiValue(PinotSegmentColumnReader columnReader, Object defaultNullValue, int numDocs,
      RoaringBitmap nullBitmap) {
    switch (columnReader.getValueType()) {
      case INT: {
        int nullValue = (Integer) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          int[] values = columnReader.getIntMV(docId);
          if (values.length == 1 && values[0] == nullValue) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case LONG: {
        long nullValue = (Long) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          long[] values = columnReader.getLongMV(docId);
          if (values.length == 1 && values[0] == nullValue) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case FLOAT: {
        float nullValue = (Float) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          float[] values = columnReader.getFloatMV(docId);
          if (values.length == 1 && Float.compare(values[0], nullValue) == 0) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case DOUBLE: {
        double nullValue = (Double) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          double[] values = columnReader.getDoubleMV(docId);
          if (values.length == 1 && Double.compare(values[0], nullValue) == 0) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal nullValue = (BigDecimal) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          BigDecimal[] values = columnReader.getBigDecimalMV(docId);
          if (values.length == 1 && values[0].compareTo(nullValue) == 0) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case STRING: {
        String nullValue = (String) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          String[] values = columnReader.getStringMV(docId);
          if (values.length == 1 && nullValue.equals(values[0])) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      case BYTES: {
        byte[] nullValue = (byte[]) defaultNullValue;
        for (int docId = 0; docId < numDocs; docId++) {
          byte[][] values = columnReader.getBytesMV(docId);
          if (values.length == 1 && Arrays.equals(values[0], nullValue)) {
            nullBitmap.add(docId);
          }
        }
        break;
      }
      default:
        throw new IllegalStateException(
            "Unsupported stored type for null value vector backfill: " + columnReader.getValueType());
    }
  }
}
