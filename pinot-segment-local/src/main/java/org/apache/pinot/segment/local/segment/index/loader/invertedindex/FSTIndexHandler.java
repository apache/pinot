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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.TextIndexCreatorProvider;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.FST_INDEX_FILE_EXTENSION;


/**
 * Helper class for fst indexes used by {@link SegmentPreProcessor}.
 * to create FST index for column during segment load time. Currently FST index is always
 * created (if enabled on a column) during segment generation
 *
 * (1) A new segment with FST index is created/refreshed. Server loads the segment. The handler
 * detects the existence of FST index and returns.
 *
 * (2) A reload is issued on an existing segment with existing FST index. The handler
 * detects the existence of FST index and returns.
 *
 * (3) A reload is issued on an existing segment after FST index is enabled on an existing
 * column. Reads the dictionary to create FST index.
 *
 * (4) A reload is issued on an existing segment after FST index is enabled on a newly
 * added column. In this case, the default column handler would have taken care of adding
 * dictionary for the new column. Read the dictionary to create FST index.
 */
public class FSTIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(FSTIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final Set<String> _columnsToAddIdx;
  private final FSTType _fstType;

  public FSTIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _fstType = indexLoadingConfig.getFSTIndexType();
    _columnsToAddIdx = new HashSet<>(indexLoadingConfig.getFSTIndexColumns());
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.FST_INDEX);
    return !existingColumns.equals(_columnsToAddIdx);
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.FST_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing FST index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.FST_INDEX);
        LOGGER.info("Removed existing FST index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        checkUnsupportedOperationsForFSTIndex(columnMetadata);
        createFSTIndexForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private void checkUnsupportedOperationsForFSTIndex(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    if (columnMetadata.getDataType() != FieldSpec.DataType.STRING) {
      throw new UnsupportedOperationException("FST index is currently only supported on STRING columns: " + column);
    }

    if (!columnMetadata.hasDictionary()) {
      throw new UnsupportedOperationException(
          "FST index is currently only supported on dictionary encoded columns: " + column);
    }

    if (!columnMetadata.isSingleValue()) {
      throw new UnsupportedOperationException("FST index is currently not supported on multi-value columns: " + column);
    }
  }

  private void createFSTIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      TextIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".fst.inprogress");
    File fstIndexFile = new File(indexDir, columnName + FST_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      FileUtils.deleteQuietly(fstIndexFile);
    }

    LOGGER.info("Creating new FST index for column: {} in segment: {}, cardinality: {}", columnName, segmentName,
        columnMetadata.getCardinality());

    TextIndexCreator fstIndexCreator = indexCreatorProvider.newTextIndexCreator(
        IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata).build()
            .forFSTIndex(_fstType, null));

    try (Dictionary dictionary = LoaderUtils.getDictionary(segmentWriter, columnMetadata)) {
      for (int dictId = 0; dictId < dictionary.length(); dictId++) {
        fstIndexCreator.add(dictionary.getStringValue(dictId));
      }
    }
    fstIndexCreator.seal();

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, fstIndexFile, ColumnIndexType.FST_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);
    LOGGER.info("Created FST index for segment: {}, column: {}", segmentName, columnName);
  }
}
