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
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexCreator;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.NATIVE_FST_INDEX_FILE_EXTENSION;

/**
 * Handler for native FST index
 */
public class NativeFSTIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeFSTIndexHandler.class);

  private final File _indexDir;
  private final SegmentMetadata _segmentMetadata;
  private final SegmentDirectory.Writer _segmentWriter;
  private final Set<String> _columnsToAddIdx;

  public NativeFSTIndexHandler(File indexDir, SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentMetadata = segmentMetadata;
    _segmentWriter = segmentWriter;
    _columnsToAddIdx = new HashSet<>(indexLoadingConfig.getNativeFSTIndexColumns());
  }

  @Override
  public void updateIndices()
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns = _segmentWriter.toSegmentDirectory().
        getColumnsWithIndex(ColumnIndexType.NATIVE_FST_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing Native FST index from segment: {}, column: {}", segmentName, column);
        _segmentWriter.removeIndex(column, ColumnIndexType.NATIVE_FST_INDEX);
        LOGGER.info("Removed existing Native FST index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        org.apache.pinot.segment.local.segment.index.loader.invertedindex.
            LuceneFSTIndexHandler.checkUnsupportedOperationsForFSTIndex(columnMetadata);
        createFSTIndexForColumn(columnMetadata);
      }
    }
  }

  private void createFSTIndexForColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String segmentName = _segmentMetadata.getName();
    String column = columnMetadata.getColumnName();
    File inProgress = new File(_indexDir, column + ".native.fst.inprogress");
    File fstIndexFile = new File(_indexDir, column + NATIVE_FST_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      FileUtils.deleteQuietly(fstIndexFile);
    }

    LOGGER.info("Creating new native FST index for column: {} in segment: {}, cardinality: {}", column, segmentName,
        columnMetadata.getCardinality());
    NativeFSTIndexCreator nativeFSTIndexCreator = new NativeFSTIndexCreator(_indexDir, column, null);
    try (Dictionary dictionary = LoaderUtils.getDictionary(_segmentWriter, columnMetadata)) {
      for (int dictId = 0; dictId < dictionary.length(); dictId++) {
        nativeFSTIndexCreator.add(dictionary.getStringValue(dictId));
      }
    }
    nativeFSTIndexCreator.seal();

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, fstIndexFile, ColumnIndexType.NATIVE_FST_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);
    LOGGER.info("Created Native FST index for segment: {}, column: {}", segmentName, column);
  }
}