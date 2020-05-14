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
package org.apache.pinot.core.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.SingleColumnMultiValueReader;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import org.apache.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.core.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RangeIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RangeIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _rangeIndexColumns = new HashSet<>();

  public RangeIndexHandler(@Nonnull File indexDir, @Nonnull SegmentMetadataImpl segmentMetadata,
      @Nonnull IndexLoadingConfig indexLoadingConfig, @Nonnull SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    // Do not create inverted index for sorted column
    for (String column : indexLoadingConfig.getRangeIndexColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        _rangeIndexColumns.add(columnMetadata);
      }
    }
  }

  public void createRangeIndices()
      throws IOException {
    for (ColumnMetadata columnMetadata : _rangeIndexColumns) {
      createRangeIndexForColumn(columnMetadata);
    }
  }

  private void createRangeIndexForColumn(ColumnMetadata columnMetadata)
      throws IOException {
    //Range index supported only for dictionary encoded columns for now
    if (!columnMetadata.hasDictionary()) {
      LOGGER.warn("Skipping creation of Range index for column:{}. It's only supported for dictionary encoded columns",
          columnMetadata.getColumnName());

      return;
    }
    String column = columnMetadata.getColumnName();

    File inProgress = new File(_indexDir, column + ".range.inprogress");
    File rangeIndexFile = new File(_indexDir, column + V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.RANGE_INDEX)) {
        // Skip creating range index if already exists.

        LOGGER.info("Found Range index for segment: {}, column: {}", _segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(rangeIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new range index for segment: {}, column: {}", _segmentName, column);
    int numDocs = columnMetadata.getTotalDocs();

    PinotDataBuffer dictBuffer = _segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    BaseImmutableDictionary dictionary = PhysicalColumnIndexContainer.loadDictionary(dictBuffer, columnMetadata, false);
    handleDictionaryBasedColumn(dictionary, columnMetadata, numDocs);

    // For v3, write the generated inverted index file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, rangeIndexFile, ColumnIndexType.RANGE_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", _segmentName, column);
  }

  private void handleDictionaryBasedColumn(BaseImmutableDictionary dictionary, ColumnMetadata columnMetadata,
      int numDocs)
      throws IOException {
    try (RangeIndexCreator creator = new RangeIndexCreator(_indexDir, columnMetadata.getFieldSpec(),
        FieldSpec.DataType.INT, -1, -1, numDocs, columnMetadata.getTotalNumberOfEntries())) {
      try (DataFileReader fwdIndex = getForwardIndexReader(columnMetadata, _segmentWriter)) {
        if (columnMetadata.isSingleValue()) {
          // Single-value column.
          FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;
          for (int i = 0; i < numDocs; i++) {
            creator.add(svFwdIndex.getInt(i));
          }
        } else {
          // Multi-value column.
          SingleColumnMultiValueReader mvFwdIndex = (SingleColumnMultiValueReader) fwdIndex;
          int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
          for (int i = 0; i < numDocs; i++) {
            int length = mvFwdIndex.getIntArray(i, dictIds);
            creator.add(dictIds, length);
          }
        }
        creator.seal();
      }
    }
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
