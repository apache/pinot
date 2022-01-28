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
import org.apache.pinot.segment.spi.creator.InvertedIndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class InvertedIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final HashSet<String> _columnsToAddIdx;

  public InvertedIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _columnsToAddIdx = new HashSet<>(indexLoadingConfig.getInvertedIndexColumns());
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.INVERTED_INDEX);
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing inverted index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (shouldCreateInvertedIndex(columnMetadata)) {
        LOGGER.info("Need to create new inverted index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws IOException {
    // Remove indices not set in table config any more.
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns =
        segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.INVERTED_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing inverted index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.INVERTED_INDEX);
        LOGGER.info("Removed existing inverted index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (shouldCreateInvertedIndex(columnMetadata)) {
        createInvertedIndexForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private boolean shouldCreateInvertedIndex(ColumnMetadata columnMetadata) {
    // Only create inverted index on dictionary-encoded unsorted columns.
    return columnMetadata != null && !columnMetadata.isSorted() && columnMetadata.hasDictionary();
  }

  private void createInvertedIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      InvertedIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".inv.inprogress");
    File invertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(invertedIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, columnName);
    int numDocs = columnMetadata.getTotalDocs();
    try (DictionaryBasedInvertedIndexCreator creator = indexCreatorProvider.newInvertedIndexCreator(
        IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata).build()
            .forInvertedIndex())) {
      try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
          ForwardIndexReaderContext readerContext = forwardIndexReader.createContext()) {
        if (columnMetadata.isSingleValue()) {
          // Single-value column.
          for (int i = 0; i < numDocs; i++) {
            creator.add(forwardIndexReader.getDictId(i, readerContext));
          }
        } else {
          // Multi-value column.
          int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
          for (int i = 0; i < numDocs; i++) {
            int length = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
            creator.add(dictIds, length);
          }
        }
        creator.seal();
      }
    }

    // For v3, write the generated inverted index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, invertedIndexFile, ColumnIndexType.INVERTED_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", segmentName, columnName);
  }
}
