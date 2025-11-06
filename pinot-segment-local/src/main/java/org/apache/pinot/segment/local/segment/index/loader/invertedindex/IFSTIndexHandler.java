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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.FSTIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.LUCENE_V912_IFST_INDEX_FILE_EXTENSION;


/**
 * Helper class for IFST (case-insensitive FST) indexes used by {@link SegmentPreProcessor}.
 * to create IFST index for column during segment load time. Currently IFST index is always
 * created (if enabled on a column) during segment generation
 *
 * (1) A new segment with IFST index is created/refreshed. Server loads the segment. The handler
 * detects the existence of IFST index and returns.
 *
 * (2) A reload is issued on an existing segment with existing IFST index. The handler
 * detects the existence of IFST index and returns.
 *
 * (3) A reload is issued on an existing segment after IFST index is enabled on an existing
 * column. Reads the dictionary to create IFST index.
 *
 * (4) A reload is issued on an existing segment after IFST index is enabled on a newly
 * added column. In this case, the default column handler would have taken care of adding
 * dictionary for the new column. Read the dictionary to create IFST index.
 */
public class IFSTIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(IFSTIndexHandler.class);
  private final Set<String> _columnsToAddIdx;

  public IFSTIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.ifst(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.ifst());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing IFST index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateIFSTIndex(columnMetadata)) {
        LOGGER.info("Need to create new IFST index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Remove indices not set in table config any more
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_columnsToAddIdx);
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.ifst());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing IFST index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.ifst());
        LOGGER.info("Removed existing IFST index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateIFSTIndex(columnMetadata)) {
        createIFSTIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
  }

  private boolean shouldCreateIFSTIndex(ColumnMetadata columnMetadata) {
    if (columnMetadata != null) {
      // Fail fast upon unsupported operations.
      checkUnsupportedOperationsForIFSTIndex(columnMetadata);
      return true;
    }
    return false;
  }

  private void checkUnsupportedOperationsForIFSTIndex(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    if (columnMetadata.getDataType() != FieldSpec.DataType.STRING) {
      throw new UnsupportedOperationException("IFST index is currently only supported on STRING columns: " + column);
    }
    if (!columnMetadata.hasDictionary()) {
      throw new UnsupportedOperationException(
          "IFST index is currently only supported on dictionary encoded columns: " + column);
    }
    if (!columnMetadata.isSingleValue()) {
      throw new UnsupportedOperationException(
          "IFST index is currently not supported on multi-value columns: " + column);
    }
  }

  private void createIFSTIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".ifst.inprogress");
    File ifstIndexFile = new File(indexDir, columnName + LUCENE_V912_IFST_INDEX_FILE_EXTENSION);
    if (!inProgress.exists()) {
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      FileUtils.deleteQuietly(ifstIndexFile);
    }
    LOGGER.info("Creating new IFST index for column: {} in segment: {}, cardinality: {}", columnName, segmentName,
        columnMetadata.getCardinality());
    IndexCreationContext context =
        IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata).build();
    FstIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.ifst());
    try (FSTIndexCreator ifstIndexCreator = StandardIndexes.ifst().createIndexCreator(context, config);
        Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata)) {
      for (int dictId = 0; dictId < dictionary.length(); dictId++) {
        ifstIndexCreator.add(dictionary.getStringValue(dictId));
      }
      ifstIndexCreator.seal();
    }
    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, ifstIndexFile, StandardIndexes.ifst());
    }
    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);
    LOGGER.info("Created IFST index for segment: {}, column: {}", segmentName, columnName);
  }
}
