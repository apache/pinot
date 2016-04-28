/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
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
  private static Logger LOGGER = LoggerFactory.getLogger(SegmentPreProcessor.class);

  private final File indexDir;
  private SegmentDirectory segmentDirectory;
  private final SegmentMetadataImpl metadata;
  private final Optional<IndexLoadingConfigMetadata> indexConfig;
  //private IndexLoadingConfigMetadata indexConfig;

  SegmentPreProcessor(File indexDir, SegmentMetadataImpl metadata,
      IndexLoadingConfigMetadata indexConfig) {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkNotNull(metadata);
    Preconditions.checkState(indexDir.exists(), "Segment directory: {} does not exist", indexDir);;
    Preconditions.checkState(indexDir.isDirectory(), "Segment path: {} is not a directory", indexDir);

    this.indexDir = indexDir;
    this.metadata = metadata;
    this.indexConfig = Optional.fromNullable(indexConfig);
    // always use mmap. That's safest and performs well without impact from
    // -Xmx params
    // This is not final load of the segment
    segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, metadata, ReadMode.mmap);
  }

  public void process()
      throws IOException {
    SegmentDirectory.Writer segmentWriter = null;
    try {
      segmentWriter = segmentDirectory.createWriter();
      addRemoveInvertedIndices(segmentWriter);
    } finally {
      if (segmentWriter != null) {
        try {
          segmentWriter.saveAndClose();
        } catch (Exception e) {
          LOGGER.error("Failed to close segment directory: {}", segmentDirectory, e);
        }
      }
    }
  }

  private void addRemoveInvertedIndices(SegmentDirectory.Writer segmentWriter)
      throws IOException {
    Set<String> invertedIndexColumnList = getInvertedIndexColumnList();

    for (String column : invertedIndexColumnList) {
      ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(column);
      createInvertedIndex(segmentWriter, metadata.getColumnMetadataFor(column));
    }
  }

  private DataFileReader getForwardIndexReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer indexBuffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    DataFileReader reader;
    if (columnMetadata.isSingleValue()) {
      SingleColumnSingleValueReader fwdIndexReader =
        new FixedBitSingleValueReader(indexBuffer, columnMetadata.getTotalDocs(),
            columnMetadata.getBitsPerElement(), columnMetadata.hasNulls());
      reader = fwdIndexReader;
    } else {
      SingleColumnMultiValueReader<? extends ReaderContext> fwdIndexReader =
          new FixedBitMultiValueReader(indexBuffer, columnMetadata.getTotalDocs(),
              columnMetadata.getTotalNumberOfEntries(), columnMetadata.getBitsPerElement(), false);
      reader = fwdIndexReader;
    }
    return reader;
  }

  private void createInvertedIndex(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws IOException {
    String segmentName = segmentWriter.toString();
    String column = columnMetadata.getColumnName();
    File inProgress = new File(segmentWriter.toSegmentDirectory().getPath().toFile(),
        column + "_inv.inprogress");

    // returning existing inverted index only if the marker file does not exist
    if (!inProgress.exists() && segmentWriter.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
      LOGGER.info("Found inverted index for segment: {}, colummn {}, loading it", segmentName, column);
      return;
    }

    // creating the marker file
    FileUtils.touch(inProgress);
    if (segmentWriter.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
      LOGGER.info("Deleting stale inverted index for segment: {}, column: {}", segmentName, column);
      segmentWriter.removeIndex(column, ColumnIndexType.INVERTED_INDEX);
    }

    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, column);

    // creating inverted index for the column now
    OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(indexDir, columnMetadata.getCardinality(), columnMetadata.getTotalDocs(),
            columnMetadata.getTotalNumberOfEntries(), columnMetadata.toFieldSpec());
    DataFileReader fwdIndex = getForwardIndexReader(columnMetadata, segmentWriter);

    if (!columnMetadata.isSingleValue()) {
      SingleColumnMultiValueReader mvFwdIndex = (SingleColumnMultiValueReader) fwdIndex;
      int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
      for (int i = 0; i < metadata.getTotalDocs(); i++) {
        int len = mvFwdIndex.getIntArray(i, dictIds);
        creator.add(i, dictIds, len);
      }
    } else {
      FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;
      for (int i = 0; i < columnMetadata.getTotalDocs(); i++) {
        creator.add(i, svFwdIndex.getInt(i));
      }
    }
    creator.seal();
    File invertedIndexFile = creator.getInvertedIndexFile();
    // Inverted index creation does not know the size upfront so
    // we create it in a temporary file and then move to the main
    // file. For v1/v2 format, this is an overkill but it's required
    // to avoid corruption of v3 format
    File tempFile = new File(invertedIndexFile + ".temp");
    if (tempFile.exists()) {
      FileUtils.deleteQuietly(tempFile);
    }
    FileUtils.moveFile(invertedIndexFile, tempFile);
    PinotDataBuffer newIndexBuffer = null;

    try {
      if (segmentWriter.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
        PinotDataBuffer tempBuffer = segmentWriter.getIndexFor(column, ColumnIndexType.INVERTED_INDEX);

        // almost always we will have matching size since segment data is immutable
        // but it's good to double check
        if (tempBuffer.size() == tempFile.length()) {
          newIndexBuffer = tempBuffer;
        } else {
          if (segmentWriter.isIndexRemovalSupported()) {
            segmentWriter.removeIndex(column, ColumnIndexType.INVERTED_INDEX);
            newIndexBuffer = segmentWriter.newIndexFor(column, ColumnIndexType.INVERTED_INDEX, (int) tempFile.length());
          } else {
            LOGGER.error("Segment: {} already has inverted index that can not be removed. Throwing exception to discard and download segment",
                segmentWriter);
            throw new IllegalStateException("Inverted Index exists and can not be removed for segment: " +
                segmentWriter +
                ". Throwing exception to download fresh segment");
          }
        }
      } else { // there was no index earlier
        newIndexBuffer = segmentWriter.newIndexFor(column,
            ColumnIndexType.INVERTED_INDEX, (int) tempFile.length());
      }
      newIndexBuffer.readFrom(tempFile);
    } finally {
      if (newIndexBuffer != null) {
        newIndexBuffer.close();
      }
    }

    // delete the marker file
    FileUtils.deleteQuietly(inProgress);
    FileUtils.deleteQuietly(tempFile);

    LOGGER.info("Created inverted index for segment: {}, colummn {}", segmentName, column);
  }

  private Set<String> getInvertedIndexColumnList() {
    Set<String> invertedIndexColumns = new HashSet<>();
    Set<String> allColumns = metadata.getAllColumns();
    Set<String> invertedIndexColumnConfig = new HashSet<>();
    if (!indexConfig.isPresent() || indexConfig.get().getLoadingInvertedIndexColumns() == null) {
      return invertedIndexColumnConfig;
    }

    Set<String> inputIIColumnList = indexConfig.get().getLoadingInvertedIndexColumns();
    for (String column : inputIIColumnList) {
      ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(column);
      if (columnMetadata == null) {
        continue;
      }
      if (! columnMetadata.isSorted()) {
        invertedIndexColumnConfig.add(column);
      }
    }
    return invertedIndexColumnConfig;
  }

  @Override
  public void close()
      throws Exception {
    segmentDirectory.close();
  }
}
