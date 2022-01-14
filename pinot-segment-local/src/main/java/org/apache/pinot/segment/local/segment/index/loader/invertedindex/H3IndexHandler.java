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
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.GeoSpatialIndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class H3IndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(H3IndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final Map<String, H3IndexConfig> _h3Configs;

  public H3IndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _h3Configs = indexLoadingConfig.getH3IndexConfigs();
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    Set<String> columnsToAddIdx = new HashSet<>(_h3Configs.keySet());
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.H3_INDEX);
    return !existingColumns.equals(columnsToAddIdx);
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    Set<String> columnsToAddIdx = new HashSet<>(_h3Configs.keySet());
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.H3_INDEX);
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing H3 index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.H3_INDEX);
        LOGGER.info("Removed existing H3 index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        createH3IndexForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private void createH3IndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      GeoSpatialIndexCreatorProvider indexCreatorProvider)
      throws Exception {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION + ".inprogress");
    File h3IndexFile = new File(indexDir, columnName + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove H3 index if exists.
      // For v1 and v2, it's the actual H3 index. For v3, it's the temporary H3 index.
      FileUtils.deleteQuietly(h3IndexFile);
    }

    // Create new H3 index for the column.
    LOGGER.info("Creating new H3 index for segment: {}, column: {}", segmentName, columnName);
    Preconditions
        .checkState(columnMetadata.getDataType() == DataType.BYTES, "H3 index can only be applied to BYTES columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    }

    // For v3, write the generated H3 index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, h3IndexFile, ColumnIndexType.H3_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created H3 index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      GeoSpatialIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    String columnName = columnMetadata.getColumnName();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = LoaderUtils.getDictionary(segmentWriter, columnMetadata);
        GeoSpatialIndexCreator h3IndexCreator = indexCreatorProvider.newGeoSpatialIndexCreator(
            IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata)
                .build().forGeospatialIndex(_h3Configs.get(columnName)))) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        int dictId = forwardIndexReader.getDictId(i, readerContext);
        h3IndexCreator.add(GeometrySerializer.deserialize(dictionary.getBytesValue(dictId)));
      }
      h3IndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      GeoSpatialIndexCreatorProvider indexCreatorProvider)
      throws Exception {
    File indexDir = _segmentMetadata.getIndexDir();
    String columnName = columnMetadata.getColumnName();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        GeoSpatialIndexCreator h3IndexCreator = indexCreatorProvider.newGeoSpatialIndexCreator(
            IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata)
                .build().forGeospatialIndex(_h3Configs.get(columnName)))) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        h3IndexCreator.add(GeometrySerializer.deserialize(forwardIndexReader.getBytes(i, readerContext)));
      }
      h3IndexCreator.seal();
    }
  }
}
