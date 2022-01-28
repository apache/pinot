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
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
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
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.BloomFilterCreatorProvider;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BloomFilterHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final Map<String, BloomFilterConfig> _bloomFilterConfigs;

  public BloomFilterHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _bloomFilterConfigs = indexLoadingConfig.getBloomFilterConfigs();
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentMetadata.getName();
    Set<String> columnsToAddBF = new HashSet<>(_bloomFilterConfigs.keySet());
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.BLOOM_FILTER);
    // Check if any existing bloomfilter need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddBF.remove(column)) {
        LOGGER.info("Need to remove existing bloom filter from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new bloomfilter need to be added.
    for (String column : columnsToAddBF) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (shouldCreateBloomFilter(columnMetadata)) {
        LOGGER.info("Need to create new bloom filter for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    Set<String> columnsToAddBF = new HashSet<>(_bloomFilterConfigs.keySet());
    // Remove indices not set in table config any more.
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.BLOOM_FILTER);
    for (String column : existingColumns) {
      if (!columnsToAddBF.remove(column)) {
        LOGGER.info("Removing existing bloom filter from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.BLOOM_FILTER);
        LOGGER.info("Removed existing bloom filter from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddBF) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (shouldCreateBloomFilter(columnMetadata)) {
        createBloomFilterForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private boolean shouldCreateBloomFilter(ColumnMetadata columnMetadata) {
    // TODO: Support raw index
    return columnMetadata != null && columnMetadata.hasDictionary();
  }

  private void createBloomFilterForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      BloomFilterCreatorProvider indexCreatorProvider)
      throws Exception {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    String columnName = columnMetadata.getColumnName();
    File bloomFilterFileInProgress = new File(indexDir, columnName + ".bloom.inprogress");
    File bloomFilterFile = new File(indexDir, columnName + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    if (!bloomFilterFileInProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(bloomFilterFileInProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove bloom filter file.
      FileUtils.deleteQuietly(bloomFilterFile);
    }

    // Create new bloom filter for the column.
    BloomFilterConfig bloomFilterConfig = _bloomFilterConfigs.get(columnName);
    LOGGER.info("Creating new bloom filter for segment: {}, column: {} with config: {}", segmentName, columnName,
        bloomFilterConfig);
    try (BloomFilterCreator bloomFilterCreator = indexCreatorProvider.newBloomFilterCreator(
        IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata)
            .build().forBloomFilter(bloomFilterConfig));
        Dictionary dictionary = getDictionaryReader(columnMetadata, segmentWriter)) {
      int length = dictionary.length();
      for (int i = 0; i < length; i++) {
        bloomFilterCreator.add(dictionary.getStringValue(i));
      }
      bloomFilterCreator.seal();
    }

    // For v3, write the generated bloom filter file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, bloomFilterFile, ColumnIndexType.BLOOM_FILTER);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(bloomFilterFileInProgress);
    LOGGER.info("Created bloom filter for segment: {}, column: {}", segmentName, columnName);
  }

  private BaseImmutableDictionary getDictionaryReader(ColumnMetadata columnMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer dictionaryBuffer =
        segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
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
        throw new IllegalStateException(
            "Unsupported data type: " + dataType + " for column: " + columnMetadata.getColumnName());
    }
  }
}
