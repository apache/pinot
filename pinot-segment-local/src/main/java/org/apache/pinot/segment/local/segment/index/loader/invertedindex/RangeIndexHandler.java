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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class RangeIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexHandler.class);

  private final Set<String> _columnsToAddIdx;

  @VisibleForTesting
  public RangeIndexHandler(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig) {
    this(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(), indexLoadingConfig.getTableConfig(),
        indexLoadingConfig.getSchema());
  }

  public RangeIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.range(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.range());

    // Check if any index updates are required.
    boolean rangeIndexUpdated = false;

    // Check if any existing index need to be removed or rebuilt due to a version change.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing range index from segment: {}, column: {}", segmentName, column);
        rangeIndexUpdated = true;
      } else if (existingRangeIndexVersionDiffers(segmentReader, column)) {
        LOGGER.info("Need to rebuild range index for segment: {}, column: {} due to version change", segmentName,
            column);
        rangeIndexUpdated = true;
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (columnMetadata == null) {
        continue;
      }
      if (columnMetadata.isSorted()) {
        LOGGER.info("Skipping creation of range index for segment: {}, column: {} as it is a sorted column",
            segmentName, column);
      } else {
        LOGGER.info("Need to create new range index for segment: {}, column: {}", segmentName, column);
        rangeIndexUpdated = true;
      }
    }
    return rangeIndexUpdated;
  }

  /// Returns `true` if the on-disk range index version doesn't match the configured version. Range index v1
  /// (RangeIndexCreator) and v2 (BitSlicedRangeIndexCreator) have incompatible on-disk layouts and serve
  /// different query semantics (v1 is non-exact, v2 is exact), so a version change requires rebuild.
  private boolean existingRangeIndexVersionDiffers(SegmentDirectory.Reader segmentReader, String column)
      throws Exception {
    int configuredVersion = _fieldIndexConfigs.get(column).getConfig(StandardIndexes.range()).getVersion();
    // The buffer is owned by SegmentDirectory; don't close it here (mmap regions are shared).
    PinotDataBuffer rangeIndexBuffer = segmentReader.getIndexFor(column, StandardIndexes.range());
    int onDiskVersion = rangeIndexBuffer.getInt(0);
    return onDiskVersion != configuredVersion;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more, or those whose on-disk version differs from the
    // configured version (v1↔v2 require rebuild).
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.range());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing range index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.range());
        LOGGER.info("Removed existing range index from segment: {}, column: {}", segmentName, column);
      } else if (existingRangeIndexVersionDiffers(segmentWriter, column)) {
        LOGGER.info("Rebuilding range index for segment: {}, column: {} due to version change", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.range());
        ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
        if (columnMetadata != null && !columnMetadata.isSorted()) {
          createRangeIndexForColumn(segmentWriter, columnMetadata);
        }
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        createRangeIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  private void createRangeIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".range.inprogress");
    File rangeIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove range index if exists.
      // For v1 and v2, it's the actual range index. For v3, it's the temporary range index.
      FileUtils.deleteQuietly(rangeIndexFile);
    }

    // Create a temporary forward index if it is disabled and does not exist
    columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);

    // Create new range index for the column.
    LOGGER.info("Creating new range index for segment: {}, column: {}", segmentName, columnName);
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata);
    }

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, rangeIndexFile, StandardIndexes.range());
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created range index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    int numDocs = columnMetadata.getTotalDocs();
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader forwardIndexReader = readerFactory.createIndexReader(segmentWriter,
        _fieldIndexConfigs.get(columnMetadata.getColumnName()), columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        CombinedInvertedIndexCreator rangeIndexCreator = newRangeIndexCreator(columnMetadata)) {
      if (forwardIndexReader.isDictionaryEncoded()) {
        if (columnMetadata.isSingleValue()) {
          for (int i = 0; i < numDocs; i++) {
            rangeIndexCreator.add(forwardIndexReader.getDictId(i, readerContext));
          }
        } else {
          int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
          for (int i = 0; i < numDocs; i++) {
            int length = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
            rangeIndexCreator.add(dictIds, length);
          }
        }
      } else {
        // RAW forward + shared standalone dictionary: read raw values and look each up in the dictionary to feed
        // dict IDs into the range index.
        try (Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
          DictionaryBasedIndexBuilder.addRawValuesViaDictionary(rangeIndexCreator, forwardIndexReader, readerContext,
              dictionary, columnMetadata, numDocs);
        }
      }
      rangeIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    int numDocs = columnMetadata.getTotalDocs();
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader forwardIndexReader = readerFactory.createIndexReader(segmentWriter,
        _fieldIndexConfigs.get(columnMetadata.getColumnName()), columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        CombinedInvertedIndexCreator rangeIndexCreator =
            newRangeIndexCreator(columnMetadata, forwardIndexReader, readerContext, numDocs)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.
        switch (columnMetadata.getDataType().getStoredType()) {
          case INT:
            for (int i = 0; i < numDocs; i++) {
              rangeIndexCreator.add(forwardIndexReader.getInt(i, readerContext));
            }
            break;
          case LONG:
            for (int i = 0; i < numDocs; i++) {
              rangeIndexCreator.add(forwardIndexReader.getLong(i, readerContext));
            }
            break;
          case FLOAT:
            for (int i = 0; i < numDocs; i++) {
              rangeIndexCreator.add(forwardIndexReader.getFloat(i, readerContext));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numDocs; i++) {
              rangeIndexCreator.add(forwardIndexReader.getDouble(i, readerContext));
            }
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + columnMetadata.getDataType());
        }
      } else {
        // Multi-value column.
        int maxNumValuesPerMVEntry = columnMetadata.getMaxNumberOfMultiValues();
        switch (columnMetadata.getDataType().getStoredType()) {
          case INT:
            int[] intValues = new int[maxNumValuesPerMVEntry];
            for (int i = 0; i < numDocs; i++) {
              int length = forwardIndexReader.getIntMV(i, intValues, readerContext);
              rangeIndexCreator.add(intValues, length);
            }
            break;
          case LONG:
            long[] longValues = new long[maxNumValuesPerMVEntry];
            for (int i = 0; i < numDocs; i++) {
              int length = forwardIndexReader.getLongMV(i, longValues, readerContext);
              rangeIndexCreator.add(longValues, length);
            }
            break;
          case FLOAT:
            float[] floatValues = new float[maxNumValuesPerMVEntry];
            for (int i = 0; i < numDocs; i++) {
              int length = forwardIndexReader.getFloatMV(i, floatValues, readerContext);
              rangeIndexCreator.add(floatValues, length);
            }
            break;
          case DOUBLE:
            double[] doubleValues = new double[maxNumValuesPerMVEntry];
            for (int i = 0; i < numDocs; i++) {
              int length = forwardIndexReader.getDoubleMV(i, doubleValues, readerContext);
              rangeIndexCreator.add(doubleValues, length);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + columnMetadata.getDataType());
        }
      }
      rangeIndexCreator.seal();
    }
  }

  private CombinedInvertedIndexCreator newRangeIndexCreator(ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    IndexCreationContext context = new IndexCreationContext.Builder(indexDir, _tableConfig, columnMetadata).build();
    RangeIndexConfig config = _fieldIndexConfigs.get(columnMetadata.getColumnName())
        .getConfig(StandardIndexes.range());
    return StandardIndexes.range().createIndexCreator(context, config);
  }

  /// Variant for the non-dictionary path. The BitSliced (v2) range index subtracts the column min for INT/LONG
  /// columns, so it needs the value domain up front. Ingestion-aggregated no-dictionary columns committed before
  /// min/max recovery report null min/max in their metadata, so recompute it here with a single scan of the forward
  /// index before building the creator. This is a deliberate extra pass over the column: {@code RangeBitmap.appender}
  /// requires the max at construction, so it cannot be folded into the add-loop that follows.
  ///
  /// Note: the recovered domain is written into the range index header (which the reader uses for the subtract-min),
  /// but it is not written back into the column metadata. For such legacy segments the reader therefore still reads a
  /// null metadata max and falls back to {@code Long.MAX_VALUE}; results stay correct (the RangeBitmap domain is
  /// self-contained) but segment-level max pruning is weaker until the segment is rebuilt. Segments sealed with the
  /// recovery in {@code MutableNoDictColumnStatistics} carry proper metadata min/max and do not hit this path.
  private CombinedInvertedIndexCreator newRangeIndexCreator(ColumnMetadata columnMetadata,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, int numDocs)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    IndexCreationContext.Builder builder = new IndexCreationContext.Builder(indexDir, _tableConfig, columnMetadata);
    RangeIndexConfig config = _fieldIndexConfigs.get(columnMetadata.getColumnName())
        .getConfig(StandardIndexes.range());
    if (config.getVersion() == BitSlicedRangeIndexCreator.VERSION && columnMetadata.isSingleValue()
        && (columnMetadata.getMinValue() == null || columnMetadata.getMaxValue() == null)) {
      Comparable[] minMax = computeRawMinMax(forwardIndexReader, readerContext,
          columnMetadata.getDataType().getStoredType(), numDocs);
      if (minMax != null) {
        builder.withMinValue(minMax[0]).withMaxValue(minMax[1]);
      }
    }
    return StandardIndexes.range().createIndexCreator(builder.build(), config);
  }

  /// Computes {@code [min, max]} for a single-value INT/LONG no-dictionary column by scanning the forward index.
  /// Returns {@code null} for stored types whose BitSliced range index does not read min/max (FLOAT/DOUBLE use the
  /// full floating-point ordinal domain) or that do not support it, and for empty columns.
  private static Comparable[] computeRawMinMax(ForwardIndexReader forwardIndexReader,
      ForwardIndexReaderContext readerContext, DataType storedType, int numDocs) {
    if (numDocs == 0) {
      return null;
    }
    switch (storedType) {
      case INT: {
        int min = forwardIndexReader.getInt(0, readerContext);
        int max = min;
        for (int i = 1; i < numDocs; i++) {
          int curr = forwardIndexReader.getInt(i, readerContext);
          min = Math.min(min, curr);
          max = Math.max(max, curr);
        }
        return new Comparable[]{min, max};
      }
      case LONG: {
        long min = forwardIndexReader.getLong(0, readerContext);
        long max = min;
        for (int i = 1; i < numDocs; i++) {
          long curr = forwardIndexReader.getLong(i, readerContext);
          min = Math.min(min, curr);
          max = Math.max(max, curr);
        }
        return new Comparable[]{min, max};
      }
      default:
        return null;
    }
  }
}
