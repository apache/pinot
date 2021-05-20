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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class JsonIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonIndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _jsonIndexColumns = new HashSet<>();

  public JsonIndexHandler(File indexDir, SegmentMetadataImpl segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    for (String column : indexLoadingConfig.getJsonIndexColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        _jsonIndexColumns.add(columnMetadata);
      }
    }
  }

  public void createJsonIndices()
      throws Exception {
    for (ColumnMetadata columnMetadata : _jsonIndexColumns) {
      createJsonIndexForColumn(columnMetadata);
    }
  }

  private void createJsonIndexForColumn(ColumnMetadata columnMetadata)
      throws Exception {
    String columnName = columnMetadata.getColumnName();

    File inProgress = new File(_indexDir, columnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION + ".inprogress");
    File jsonIndexFile = new File(_indexDir, columnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(columnName, ColumnIndexType.JSON_INDEX)) {
        // Skip creating json index if already exists.

        LOGGER.info("Found json index for segment: {}, column: {}", _segmentName, columnName);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove json index if exists.
      // For v1 and v2, it's the actual json index. For v3, it's the temporary json index.
      FileUtils.deleteQuietly(jsonIndexFile);
    }

    // Create new json index for the column.
    LOGGER.info("Creating new json index for segment: {}, column: {}", _segmentName, columnName);
    Preconditions.checkState(columnMetadata.isSingleValue() && columnMetadata.getDataType() == DataType.STRING,
        "Json index can only be applied to single-value STRING columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(columnMetadata);
    }

    // For v3, write the generated json index file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, columnName, jsonIndexFile, ColumnIndexType.JSON_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created json index for segment: {}, column: {}", _segmentName, columnName);
    PropertiesConfiguration properties = SegmentMetadataImpl.getPropertiesConfiguration(_indexDir);
    properties.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(columnName, V1Constants.MetadataKeys.Column.HAS_JSON_INDEX), true);
    properties.save();
  }

  private void handleDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String columnName = columnMetadata.getColumnName();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(_segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = LoaderUtils.getDictionary(_segmentWriter, columnMetadata);
        OffHeapJsonIndexCreator jsonIndexCreator = new OffHeapJsonIndexCreator(_indexDir, columnName)) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        int dictId = forwardIndexReader.getDictId(i, readerContext);
        jsonIndexCreator.add(dictionary.getStringValue(dictId));
      }
      jsonIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String columnName = columnMetadata.getColumnName();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(_segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        OffHeapJsonIndexCreator jsonIndexCreator = new OffHeapJsonIndexCreator(_indexDir, columnName)) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        jsonIndexCreator.add(forwardIndexReader.getString(i, readerContext));
      }
      jsonIndexCreator.seal();
    }
  }
}
