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
package com.linkedin.pinot.core.segment.index.loader.invertedindex;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvertedIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _invertedIndexColumns = new HashSet<>();

  public InvertedIndexHandler(@Nonnull File indexDir, @Nonnull SegmentMetadataImpl segmentMetadata,
      @Nonnull IndexLoadingConfig indexLoadingConfig, @Nonnull SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    // Do not create inverted index for sorted column
    for (String column : indexLoadingConfig.getInvertedIndexColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        _invertedIndexColumns.add(columnMetadata);
      }
    }
  }

  public void createInvertedIndices() throws IOException {
    for (ColumnMetadata columnMetadata : _invertedIndexColumns) {
      createInvertedIndexForColumn(columnMetadata);
    }
  }

  private void createInvertedIndexForColumn(ColumnMetadata columnMetadata) throws IOException {
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
      try (DataFileReader fwdIndex = getForwardIndexReader(columnMetadata, _segmentWriter)) {
        if (columnMetadata.isSingleValue()) {
          // Single-value column.

          FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;
          for (int i = 0; i < numDocs; i++) {
            creator.addSV(i, svFwdIndex.getInt(i));
          }
        } else {
          // Multi-value column.

          SingleColumnMultiValueReader mvFwdIndex = (SingleColumnMultiValueReader) fwdIndex;
          int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
          for (int i = 0; i < numDocs; i++) {
            int numDictIds = mvFwdIndex.getIntArray(i, dictIds);
            creator.addMV(i, dictIds, numDictIds);
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

  private DataFileReader getForwardIndexReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    int numRows = columnMetadata.getTotalDocs();
    int numBitsPerValue = columnMetadata.getBitsPerElement();
    if (columnMetadata.isSingleValue()) {
      return new FixedBitSingleValueReader(buffer, numRows, numBitsPerValue);
    } else {
      return new FixedBitMultiValueReader(buffer, numRows, columnMetadata.getTotalNumberOfEntries(), numBitsPerValue);
    }
  }
}
