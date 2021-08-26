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
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
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
public class LuceneFSTIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneFSTIndexHandler.class);

  private final File _indexDir;
  private final SegmentMetadata _segmentMetadata;
  private final SegmentDirectory.Writer _segmentWriter;
  private final Set<String> _columnsToAddIdx;

  public LuceneFSTIndexHandler(File indexDir, SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentMetadata = segmentMetadata;
    _segmentWriter = segmentWriter;
    _columnsToAddIdx = new HashSet<>(indexLoadingConfig.getFSTIndexColumns());
  }

  @Override
  public void updateIndices()
      throws Exception {
    // Remove indices not set in table config any more
    Set<String> existingColumns = _segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.FST_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        _segmentWriter.removeIndex(column, ColumnIndexType.FST_INDEX);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        checkUnsupportedOperationsForFSTIndex(columnMetadata);
        createFSTIndexForColumn(columnMetadata);
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

  private void createFSTIndexForColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String segmentName = _segmentMetadata.getName();
    String column = columnMetadata.getColumnName();
    File inProgress = new File(_indexDir, column + ".fst.inprogress");
    File fstIndexFile = new File(_indexDir, column + FST_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.FST_INDEX)) {
        // Skip creating fst index if already exists.
        LOGGER.info("Found fst index for column: {}, in segment: {}", column, segmentName);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      FileUtils.deleteQuietly(fstIndexFile);
    }

    LOGGER.info("Creating new FST index for column: {} in segment: {}, cardinality: {}", column, segmentName,
        columnMetadata.getCardinality());
    LuceneFSTIndexCreator luceneFSTIndexCreator = new LuceneFSTIndexCreator(_indexDir, column, null);
    try (Dictionary dictionary = LoaderUtils.getDictionary(_segmentWriter, columnMetadata)) {
      for (int dictId = 0; dictId < dictionary.length(); dictId++) {
        luceneFSTIndexCreator.add(dictionary.getStringValue(dictId));
      }
    }
    luceneFSTIndexCreator.seal();

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, fstIndexFile, ColumnIndexType.FST_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);
    LOGGER.info("Created FST index for segment: {}, column: {}", segmentName, column);
  }
}
