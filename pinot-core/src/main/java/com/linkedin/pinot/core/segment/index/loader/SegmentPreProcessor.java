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
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * mmap()'s the segment and performs any pre-processing to generate inverted index
 * This can be slow
 */
public class SegmentPreProcessor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreProcessor.class);

  private final File indexDir;
  private final SegmentMetadataImpl segmentMetadata;
  private final String segmentName;
  private final SegmentVersion segmentVersion;
  private final IndexLoadingConfigMetadata indexConfig;
  private final SegmentDirectory segmentDirectory;

  SegmentPreProcessor(File indexDir, SegmentMetadataImpl segmentMetadata, IndexLoadingConfigMetadata indexConfig) {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkNotNull(segmentMetadata);
    Preconditions.checkState(indexDir.exists(), "Segment directory: {} does not exist", indexDir);
    Preconditions.checkState(indexDir.isDirectory(), "Segment path: {} is not a directory", indexDir);

    this.indexDir = indexDir;
    this.segmentMetadata = segmentMetadata;
    segmentName = segmentMetadata.getName();
    segmentVersion = SegmentVersion.valueOf(this.segmentMetadata.getVersion());
    this.indexConfig = indexConfig;
    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap);
  }

  public void process()
      throws IOException {
    SegmentDirectory.Writer segmentWriter = null;
    try {
      segmentWriter = segmentDirectory.createWriter();
      addRemoveInvertedIndices(segmentWriter);
    } finally {
      if (segmentWriter != null) {
        segmentWriter.saveAndClose();
      }
    }
  }

  private void addRemoveInvertedIndices(SegmentDirectory.Writer segmentWriter)
      throws IOException {
    Set<String> invertedIndexColumns = getInvertedIndexColumns();

    for (String column : invertedIndexColumns) {
      createInvertedIndex(segmentWriter, segmentMetadata.getColumnMetadataFor(column));
    }
  }

  private Set<String> getInvertedIndexColumns() {
    Set<String> invertedIndexColumns = new HashSet<>();
    if (indexConfig == null) {
      return invertedIndexColumns;
    }

    Set<String> invertedIndexColumnsFromConfig = indexConfig.getLoadingInvertedIndexColumns();
    for (String column : invertedIndexColumnsFromConfig) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        invertedIndexColumns.add(column);
      }
    }

    return invertedIndexColumns;
  }

  private void createInvertedIndex(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws IOException {
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(segmentWriter.toSegmentDirectory().getPath().toFile(), columnName + ".inv.inprogress");

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (segmentWriter.hasIndexFor(columnName, ColumnIndexType.INVERTED_INDEX)) {
        // Skip creating inverted index if already exists.

        LOGGER.info("Found inverted index for segment: {}, column: {}", segmentName, columnName);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove inverted index for v1 and v2.
      // For v3, try to reuse the inverted index buffer because v3 does not support removing index.
      if (segmentVersion == SegmentVersion.v1 || segmentVersion == SegmentVersion.v2) {
        segmentWriter.removeIndex(columnName, ColumnIndexType.INVERTED_INDEX);
      }
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, columnName);
    int totalDocs = columnMetadata.getTotalDocs();
    OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(indexDir, columnMetadata.getCardinality(), totalDocs,
            columnMetadata.getTotalNumberOfEntries(), columnMetadata.toFieldSpec());

    try (DataFileReader fwdIndex = getForwardIndexReader(columnMetadata, segmentWriter)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.

        FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;
        for (int i = 0; i < totalDocs; i++) {
          creator.add(i, svFwdIndex.getInt(i));
        }
      } else {
        // Multi-value column.

        SingleColumnMultiValueReader mvFwdIndex = (SingleColumnMultiValueReader) fwdIndex;
        int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
        for (int i = 0; i < totalDocs; i++) {
          int len = mvFwdIndex.getIntArray(i, dictIds);
          creator.add(i, dictIds, len);
        }
      }
    }

    creator.seal();

    // For v3, write the generated inverted index file into the single file and remove it.
    if (segmentVersion == SegmentVersion.v3) {
      File invertedIndexFile = creator.getInvertedIndexFile();
      int fileLength = (int) invertedIndexFile.length();
      PinotDataBuffer buffer = null;
      try {
        if (segmentWriter.hasIndexFor(columnName, ColumnIndexType.INVERTED_INDEX)) {
          // Inverted index already exists, try to reuse it.

          buffer = segmentWriter.getIndexFor(columnName, ColumnIndexType.INVERTED_INDEX);
          if (buffer.size() != fileLength) {
            // Existed inverted index size does not equal new generated inverted index size.
            // Throw an exception here to trigger segment drop and re-download.

            String failureMessage = "V3 format segment: {} already has inverted index that cannot be removed.";
            LOGGER.error(failureMessage);
            throw new IllegalStateException(failureMessage);
          }
        } else {
          // Inverted index does not exist, create a new buffer for that.

          buffer = segmentWriter.newIndexFor(columnName, ColumnIndexType.INVERTED_INDEX, fileLength);
        }

        buffer.readFrom(invertedIndexFile);
      } finally {
        FileUtils.deleteQuietly(invertedIndexFile);
        if (buffer != null) {
          buffer.close();
        }
      }
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", segmentName, columnName);
  }

  private DataFileReader getForwardIndexReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    if (columnMetadata.isSingleValue()) {
      return new FixedBitSingleValueReader(buffer, columnMetadata.getTotalDocs(), columnMetadata.getBitsPerElement(),
          columnMetadata.hasNulls());
    } else {
      return new FixedBitMultiValueReader(buffer, columnMetadata.getTotalDocs(),
          columnMetadata.getTotalNumberOfEntries(), columnMetadata.getBitsPerElement(), false);
    }
  }

  @Override
  public void close()
      throws Exception {
    segmentDirectory.close();
  }
}
