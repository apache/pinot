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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VectorIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexHandler.class);

  private final Map<String, VectorIndexConfig> _vectorConfigs;

  public VectorIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _vectorConfigs = FieldIndexConfigsUtil.enableConfigByColumn(StandardIndexes.vector(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_vectorConfigs.keySet());
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.vector());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing Vector index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateVectorIndex(columnMetadata)) {
        LOGGER.info("Need to create new Vector index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    Set<String> columnsToAddIdx = new HashSet<>(_vectorConfigs.keySet());
    // Remove indices not set in table config any more
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.vector());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing Vector index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.vector());
        LOGGER.info("Removed existing Vector index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateVectorIndex(columnMetadata)) {
        createVectorIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  private boolean shouldCreateVectorIndex(ColumnMetadata columnMetadata) {
    return columnMetadata != null;
  }

  private void createVectorIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    String columnName = columnMetadata.getColumnName();
    File inProgress =
        new File(segmentDirectory, columnName + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION + ".inprogress");
    File vectorIndexFile =
        new File(segmentDirectory, columnName + V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove Vector index if exists.
      // For v1 and v2, it's the actual Vector index. For v3, it's the temporary Vector index.
      FileUtils.deleteQuietly(vectorIndexFile);
    }

    // Create a temporary forward index if it is disabled and does not exist
    columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);

    // Create new Vector index for the column.
    LOGGER.info("Creating new Vector index for segment: {}, column: {}", segmentName, columnName);
    Preconditions.checkState(columnMetadata.getDataType() == FieldSpec.DataType.FLOAT,
        "VECTOR index can only be applied to Float Array columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata);
    }

//    // For v3, write the generated Vector index file into the single file and remove it.
//    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
//      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, vectorIndexFile, StandardIndexes.vector());
//    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created Vector index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String columnName = columnMetadata.getColumnName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    FieldIndexConfigs colIndexConf = _fieldIndexConfigs.get(columnName);

    IndexCreationContext context = IndexCreationContext.builder()
        .withIndexDir(segmentDirectory)
        .withColumnMetadata(columnMetadata)
        .build();
    VectorIndexConfig config = colIndexConf.getConfig(StandardIndexes.vector());

    try (ForwardIndexReader forwardIndexReader = StandardIndexes.forward().getReaderFactory()
        .createIndexReader(segmentWriter, colIndexConf, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = StandardIndexes.dictionary().getReaderFactory()
            .createIndexReader(segmentWriter, colIndexConf, columnMetadata);
        VectorIndexCreator vectorIndexCreator = StandardIndexes.vector().createIndexCreator(context, config)) {
      int numDocs = columnMetadata.getTotalDocs();
      float[] vector = new float[columnMetadata.getMaxNumberOfMultiValues()];
      int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
      for (int i = 0; i < numDocs; i++) {
        forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
        for (int j = 0; j < dictIds.length; j++) {
          vector[j] = dictionary.getFloatValue(dictIds[j]);
        }
        vectorIndexCreator.add(vector);
      }
      vectorIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String columnName = columnMetadata.getColumnName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    IndexCreationContext context = IndexCreationContext.builder()
        .withIndexDir(segmentDirectory)
        .withColumnMetadata(columnMetadata)
        .build();
    VectorIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.vector());
    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        VectorIndexCreator vectorIndexCreator = StandardIndexes.vector().createIndexCreator(context, config)) {
      int numDocs = columnMetadata.getTotalDocs();
      float[] vector = new float[columnMetadata.getMaxNumberOfMultiValues()];
      for (int i = 0; i < numDocs; i++) {
        forwardIndexReader.getFloatMV(i, vector, readerContext);
      }
      vectorIndexCreator.seal();
    }
  }
}
