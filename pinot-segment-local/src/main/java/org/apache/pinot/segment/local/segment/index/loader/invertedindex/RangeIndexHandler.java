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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.RangeIndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class RangeIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final Set<String> _columnsToAddIdx;
  private final int _rangeIndexVersion;

  public RangeIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _columnsToAddIdx = new HashSet<>(indexLoadingConfig.getRangeIndexColumns());
    _rangeIndexVersion = indexLoadingConfig.getRangeIndexVersion();
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.RANGE_INDEX);
    return !existingColumns.equals(_columnsToAddIdx);
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws IOException {
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.RANGE_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing range index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.RANGE_INDEX);
        LOGGER.info("Removed existing range index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      // Only create range index on dictionary-encoded unsorted columns
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        createRangeIndexForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private void createRangeIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      RangeIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
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

    // Create new range index for the column.
    LOGGER.info("Creating new range index for segment: {}, column: {}", segmentName, columnName);
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    }

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, rangeIndexFile, ColumnIndexType.RANGE_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created range index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      RangeIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        CombinedInvertedIndexCreator rangeIndexCreator = newRangeIndexCreator(columnMetadata, indexCreatorProvider)) {
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

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      RangeIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        CombinedInvertedIndexCreator rangeIndexCreator = newRangeIndexCreator(columnMetadata, indexCreatorProvider)) {
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

  private CombinedInvertedIndexCreator newRangeIndexCreator(ColumnMetadata columnMetadata,
      RangeIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    return indexCreatorProvider.newRangeIndexCreator(
        IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata).build()
            .forRangeIndex(_rangeIndexVersion, columnMetadata.getMinValue(), columnMetadata.getMaxValue()));
  }
}
