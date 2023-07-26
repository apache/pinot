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
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class RangeIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexHandler.class);

  private final Set<String> _columnsToAddIdx;

  @VisibleForTesting
  public RangeIndexHandler(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig) {
    super(segmentDirectory, indexLoadingConfig);
    _columnsToAddIdx = indexLoadingConfig.getRangeIndexColumns();
  }

  public RangeIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.range(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.range());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing range index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateRangeIndex(columnMetadata)) {
        LOGGER.info("Need to create new range index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.range());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing range index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.range());
        LOGGER.info("Removed existing range index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateRangeIndex(columnMetadata)) {
        createRangeIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  private boolean shouldCreateRangeIndex(ColumnMetadata columnMetadata) {
    // Only create range index on unsorted columns
    return columnMetadata != null && !columnMetadata.isSorted();
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
    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        CombinedInvertedIndexCreator rangeIndexCreator = newRangeIndexCreator(columnMetadata)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column
        for (int i = 0; i < numDocs; i++) {
          rangeIndexCreator.add(forwardIndexReader.getDictId(i, readerContext));
        }
      } else {
        // Multi-value column
        int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
        for (int i = 0; i < numDocs; i++) {
          int length = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
          rangeIndexCreator.add(dictIds, length);
        }
      }
      rangeIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        CombinedInvertedIndexCreator rangeIndexCreator = newRangeIndexCreator(columnMetadata)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.
        switch (columnMetadata.getDataType()) {
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
        // Multi-value column
        int maxNumValuesPerMVEntry = columnMetadata.getMaxNumberOfMultiValues();
        switch (columnMetadata.getDataType()) {
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
    IndexCreationContext context = IndexCreationContext.builder()
        .withIndexDir(indexDir)
        .withColumnMetadata(columnMetadata)
        .build();
    RangeIndexConfig config = _fieldIndexConfigs.get(columnMetadata.getColumnName())
        .getConfig(StandardIndexes.range());
    return StandardIndexes.range().createIndexCreator(context, config);
  }
}
