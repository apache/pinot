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
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class InvertedIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _invertedIndexColumns = new HashSet<>();

  public InvertedIndexHandler(File indexDir, SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = segmentMetadata.getVersion();

    // Only create inverted index on dictionary-encoded unsorted columns
    Set<String> columnsInCfg = indexLoadingConfig.getInvertedIndexColumns();
    for (String column : columnsInCfg) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted() && columnMetadata.hasDictionary()) {
        _invertedIndexColumns.add(columnMetadata);
      }
    }
    // Remove indices not set in table config any more
    Set<String> localColumns = _segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.INVERTED_INDEX);
    for (String column : localColumns) {
      if (!columnsInCfg.contains(column)) {
        _segmentWriter.removeIndex(column, ColumnIndexType.INVERTED_INDEX);
      }
    }
  }

  public void createInvertedIndices()
      throws IOException {
    for (ColumnMetadata columnMetadata : _invertedIndexColumns) {
      createInvertedIndexForColumn(columnMetadata);
    }
  }

  private void createInvertedIndexForColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String column = columnMetadata.getColumnName();

    File inProgress = new File(_indexDir, column + ".inv.inprogress");
    File invertedIndexFile = new File(_indexDir, column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
        // Skip creating inverted index if already exists.

        LOGGER.info("Found inverted index for segment: {}, column: {}", _segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(invertedIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", _segmentName, column);
    int numDocs = columnMetadata.getTotalDocs();
    try (OffHeapBitmapInvertedIndexCreator creator = new OffHeapBitmapInvertedIndexCreator(_indexDir,
        columnMetadata.getFieldSpec(), columnMetadata.getCardinality(), numDocs,
        columnMetadata.getTotalNumberOfEntries())) {
      try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(_segmentWriter, columnMetadata);
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
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, invertedIndexFile, ColumnIndexType.INVERTED_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", _segmentName, column);
  }
}
