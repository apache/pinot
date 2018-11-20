/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.loader.bloomfilter;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.bloom.BloomFilterCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import com.linkedin.pinot.core.segment.index.loader.invertedindex.InvertedIndexHandler;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;

public class BloomFilterHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(InvertedIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _bloomFilterColumns = new HashSet<>();

  public BloomFilterHandler(@Nonnull File indexDir, @Nonnull SegmentMetadataImpl segmentMetadata, @Nonnull IndexLoadingConfig indexLoadingConfig,
      @Nonnull SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    // Do not create inverted index for sorted column
    for (String column : indexLoadingConfig.getBloomFilterColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        _bloomFilterColumns.add(columnMetadata);
      }
    }
  }

  public void createBloomFilters() throws Exception {
    for (ColumnMetadata columnMetadata : _bloomFilterColumns) {
      if(columnMetadata.hasDictionary()) {
        createBloomFilterForColumn(columnMetadata);
      }
    }
  }

  private void createBloomFilterForColumn(ColumnMetadata columnMetadata) throws Exception {
    String column = columnMetadata.getColumnName();

    File inProgress = new File(_indexDir, column + ".bloom.inprogress");
    File bloomFilterFile = new File(_indexDir, column + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.BLOOM_FILTER)) {
        // Skip creating bloom filter index if already exists.

        LOGGER.info("Found bloom filter for segment: {}, column: {}", _segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(bloomFilterFile);
    }

    // Create new bloom filter for the column.
    LOGGER.info("Creating new bloom filter for segment: {}, column: {}", _segmentName, column);
    int numDocs = columnMetadata.getTotalDocs();
    try (BloomFilterCreator creator = new BloomFilterCreator(_indexDir, columnMetadata.getFieldSpec(), columnMetadata.getCardinality(), numDocs,
        columnMetadata.getTotalNumberOfEntries())) {
      if (columnMetadata.hasDictionary()) {
        //read dictionary
        try (ImmutableDictionaryReader dictionaryReader = getDictionaryReader(columnMetadata, _segmentWriter)) {
          for (int i = 0; i < dictionaryReader.length(); i++) {
            creator.add(dictionaryReader.get(i));
          }
        }
      } else {
        //read the forward index
        throw new UnsupportedOperationException("Bloom filters not supported for No Dictionary columns");
      }
    }

    // For v3, write the generated inverted index file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, bloomFilterFile, ColumnIndexType.BLOOM_FILTER);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created bloom filter for segment: {}, column: {}", _segmentName, column);
  }



  private ImmutableDictionaryReader getDictionaryReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter) throws IOException {
    PinotDataBuffer dictionaryBuffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    int cardinality = columnMetadata.getCardinality();
    ImmutableDictionaryReader dictionaryReader;
    DataType dataType = columnMetadata.getDataType();
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
      dictionaryReader = new StringDictionary(dictionaryBuffer, cardinality, columnMetadata.getColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter());
      break;
    default:
      throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnMetadata.getColumnName());
    }
    return dictionaryReader;
  }

}
