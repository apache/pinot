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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.JsonIndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class JsonIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final HashSet<String> _columnsToAddIdx;

  public JsonIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _columnsToAddIdx = new HashSet<>(indexLoadingConfig.getJsonIndexColumns());
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.JSON_INDEX);
    return !existingColumns.equals(_columnsToAddIdx);
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.JSON_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing json index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.JSON_INDEX);
        LOGGER.info("Removed existing json index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        createJsonIndexForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private void createJsonIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      JsonIndexCreatorProvider indexCreatorProvider)
      throws Exception {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION + ".inprogress");
    File jsonIndexFile = new File(indexDir, columnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove json index if exists.
      // For v1 and v2, it's the actual json index. For v3, it's the temporary json index.
      FileUtils.deleteQuietly(jsonIndexFile);
    }

    // Create new json index for the column.
    LOGGER.info("Creating new json index for segment: {}, column: {}", segmentName, columnName);
    Preconditions.checkState(columnMetadata.isSingleValue() && (columnMetadata.getDataType() == DataType.STRING
            || columnMetadata.getDataType() == DataType.JSON),
        "Json index can only be applied to single-value STRING or JSON columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    }

    // For v3, write the generated json index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, jsonIndexFile, ColumnIndexType.JSON_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created json index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      JsonIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = LoaderUtils.getDictionary(segmentWriter, columnMetadata);
        JsonIndexCreator jsonIndexCreator = indexCreatorProvider.newJsonIndexCreator(IndexCreationContext.builder()
            .withIndexDir(indexDir).withColumnMetadata(columnMetadata).build().forJsonIndex())) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        int dictId = forwardIndexReader.getDictId(i, readerContext);
        jsonIndexCreator.add(dictionary.getStringValue(dictId));
      }
      jsonIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      JsonIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        JsonIndexCreator jsonIndexCreator = indexCreatorProvider.newJsonIndexCreator(IndexCreationContext.builder()
            .withIndexDir(indexDir).withColumnMetadata(columnMetadata).build().forJsonIndex())) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        jsonIndexCreator.add(forwardIndexReader.getString(i, readerContext));
      }
      jsonIndexCreator.seal();
    }
  }
}
