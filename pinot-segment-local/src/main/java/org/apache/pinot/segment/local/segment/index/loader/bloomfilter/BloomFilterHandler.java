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
package org.apache.pinot.segment.local.segment.index.loader.bloomfilter;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BloomFilterHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Map<String, BloomFilterConfig> _bloomFilterConfigs;
  private final Set<ColumnMetadata> _bloomFilterColumns = new HashSet<>();

  public BloomFilterHandler(File indexDir, SegmentMetadataImpl segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = segmentMetadata.getVersion();
    _bloomFilterConfigs = indexLoadingConfig.getBloomFilterConfigs();

    for (String column : _bloomFilterConfigs.keySet()) {
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
      // TODO: Support raw index
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
    BloomFilterConfig bloomFilterConfig = _bloomFilterConfigs.get(columnName);
    LOGGER.info("Creating new bloom filter for segment: {}, column: {} with config: {}", _segmentName, columnName, bloomFilterConfig);
    try (BloomFilterCreator bloomFilterCreator = new OnHeapGuavaBloomFilterCreator(_indexDir, columnName, columnMetadata.getCardinality(),
        bloomFilterConfig); Dictionary dictionary = getDictionaryReader(columnMetadata, _segmentWriter)) {
      int length = dictionary.length();
      for (int i = 0; i < length; i++) {
        bloomFilterCreator.add(dictionary.getStringValue(i));
      }
      bloomFilterCreator.seal();
    }

    // For v3, write the generated bloom filter file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, columnName, bloomFilterFile, ColumnIndexType.BLOOM_FILTER);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(bloomFilterFileInProgress);
    LOGGER.info("Created bloom filter for segment: {}, column: {}", _segmentName, columnName);
  }

  private BaseImmutableDictionary getDictionaryReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer dictionaryBuffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    int cardinality = columnMetadata.getCardinality();
    DataType dataType = columnMetadata.getDataType();
    switch (dataType) {
      case INT:
        return new IntDictionary(dictionaryBuffer, cardinality);
      case LONG:
        return new LongDictionary(dictionaryBuffer, cardinality);
      case FLOAT:
        return new FloatDictionary(dictionaryBuffer, cardinality);
      case DOUBLE:
        return new DoubleDictionary(dictionaryBuffer, cardinality);
      case STRING:
        return new StringDictionary(dictionaryBuffer, cardinality, columnMetadata.getColumnMaxLength(),
            (byte) columnMetadata.getPaddingCharacter());
      case BYTES:
        return new BytesDictionary(dictionaryBuffer, cardinality, columnMetadata.getColumnMaxLength());
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnMetadata.getColumnName());
    }
  }
}
