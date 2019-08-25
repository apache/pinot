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
package org.apache.pinot.core.segment.index.loader.presencevector;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.bloom.BloomFilterCreator;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.core.segment.index.readers.DoubleDictionary;
import org.apache.pinot.core.segment.index.readers.FloatDictionary;
import org.apache.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import org.apache.pinot.core.segment.index.readers.IntDictionary;
import org.apache.pinot.core.segment.index.readers.LongDictionary;
import org.apache.pinot.core.segment.index.readers.StringDictionary;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PresenceVectorHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _bloomFilterColumns = new HashSet<>();

  public PresenceVectorHandler(@Nonnull File indexDir, @Nonnull SegmentMetadataImpl segmentMetadata,
      @Nonnull IndexLoadingConfig indexLoadingConfig, @Nonnull SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    for (String column : indexLoadingConfig.getBloomFilterColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        _bloomFilterColumns.add(columnMetadata);
      }
    }
  }

  public void createBloomFilters()
      throws Exception {
    for (ColumnMetadata columnMetadata : _bloomFilterColumns) {
      if (columnMetadata.hasDictionary()) {
        createBloomFilterForColumn(columnMetadata);
      }
    }
  }

  private void createBloomFilterForColumn(ColumnMetadata columnMetadata)
      throws Exception {
    String columnName = columnMetadata.getColumnName();

    File bloomFilterFileInProgress = new File(_indexDir, columnName + ".bloom.inprogress");
    File bloomFilterFile = new File(_indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    if (!bloomFilterFileInProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      if (_segmentWriter.hasIndexFor(columnName, ColumnIndexType.BLOOM_FILTER)) {
        // Skip creating bloom filter index if already exists.
        LOGGER.info("Found bloom filter for segment: {}, column: {}", _segmentName, columnName);
        return;
      }
      // Create a marker file.
      FileUtils.touch(bloomFilterFileInProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove bloom filter file.
      FileUtils.deleteQuietly(bloomFilterFile);
    }

    // Create new bloom filter for the column.
    LOGGER.info("Creating new bloom filter for segment: {}, column: {}", _segmentName, columnName);
    try (BloomFilterCreator creator = new BloomFilterCreator(_indexDir, columnName, columnMetadata.getCardinality())) {
      if (columnMetadata.hasDictionary()) {
        // Read dictionary
        try (ImmutableDictionaryReader dictionaryReader = getDictionaryReader(columnMetadata, _segmentWriter)) {
          for (int i = 0; i < dictionaryReader.length(); i++) {
            creator.add(dictionaryReader.get(i));
          }
        }
      } else {
        // Read the forward index
        throw new UnsupportedOperationException("Bloom filters not supported for no dictionary columns");
      }
    }

    // For v3, write the generated bloom filter file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, columnName, bloomFilterFile, ColumnIndexType.BLOOM_FILTER);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(bloomFilterFileInProgress);
    LOGGER.info("Created bloom filter for segment: {}, column: {}", _segmentName, columnName);
  }

  private ImmutableDictionaryReader getDictionaryReader(ColumnMetadata columnMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer dictionaryBuffer =
        segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    int cardinality = columnMetadata.getCardinality();
    ImmutableDictionaryReader dictionaryReader;
    FieldSpec.DataType dataType = columnMetadata.getDataType();
    switch (dataType) {
      case INT:
        dictionaryReader = new IntDictionary(dictionaryBuffer, cardinality);
        break;
      case LONG:
        dictionaryReader = new LongDictionary(dictionaryBuffer, cardinality);
        break;
      case FLOAT:
        dictionaryReader = new FloatDictionary(dictionaryBuffer, cardinality);
        break;
      case DOUBLE:
        dictionaryReader = new DoubleDictionary(dictionaryBuffer, cardinality);
        break;
      case STRING:
        dictionaryReader = new StringDictionary(dictionaryBuffer, cardinality, columnMetadata.getColumnMaxLength(),
            (byte) columnMetadata.getPaddingCharacter());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported data type: " + dataType + " for column: " + columnMetadata.getColumnName());
    }
    return dictionaryReader;
  }
}
