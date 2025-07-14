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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGenerator;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.MultiColumnTextIndexHandler;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
 * <p>Pre-processing steps include:
 * <ul>
 *   <li>Use {@link InvertedIndexHandler} to create inverted indices</li>
 *   <li>Use {@link DefaultColumnHandler} to update auto-generated default columns</li>
 *   <li>Use {@link ColumnMinMaxValueGenerator} to add min/max value to column metadata</li>
 * </ul>
 */
public class SegmentPreProcessor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreProcessor.class);

  private final SegmentDirectory _segmentDirectory;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final TableConfig _tableConfig;
  private final Schema _schema;

  public SegmentPreProcessor(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig) {
    _segmentDirectory = segmentDirectory;
    _indexLoadingConfig = indexLoadingConfig;
    _tableConfig = indexLoadingConfig.getTableConfig();
    Preconditions.checkArgument(_tableConfig != null, "Table config must be provided");
    _schema = indexLoadingConfig.getSchema();
    Preconditions.checkArgument(_schema != null, "Schema must be provided");
  }

  @Override
  public void close()
      throws Exception {
    _segmentDirectory.close();
  }

  public void process()
      throws Exception {
    process(null);
  }

  // TODO: Reduce segment metadata reload, and reload it only if it is modified.
  public void process(@Nullable SegmentOperationsThrottler segmentOperationsThrottler)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    if (segmentMetadata.getTotalDocs() == 0) {
      LOGGER.info("Skip preprocessing empty segment: {}", segmentName);
      return;
    }

    // Segment processing has to be done with a local directory.
    File indexDir = new File(_segmentDirectory.getIndexDir());

    // This fixes the issue of temporary files not getting deleted after creating new inverted indexes.
    removeInvertedIndexTempFiles(indexDir);

    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      // Update default columns according to the schema.
      DefaultColumnHandler defaultColumnHandler =
          DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir, segmentMetadata, _indexLoadingConfig,
              segmentWriter);
      defaultColumnHandler.updateDefaultColumns();
      _segmentDirectory.reloadMetadata();

      // Update single-column indices, like inverted index, json index etc.
      List<IndexHandler> indexHandlers = new ArrayList<>();

      // We cannot just create all the index handlers in a random order.
      // Specifically, ForwardIndexHandler needs to be executed first. This is because it modifies the segment metadata
      // while rewriting forward index to create a dictionary. Some other handlers (like the range one) assume that
      // metadata was already been modified by ForwardIndexHandler.
      IndexHandler forwardHandler = createHandler(StandardIndexes.forward());
      indexHandlers.add(forwardHandler);
      forwardHandler.updateIndices(segmentWriter);
      _segmentDirectory.reloadMetadata();

      // Now that ForwardIndexHandler.updateIndices has been updated, we can run all other indexes in any order
      for (IndexType<?, ?, ?> type : IndexService.getInstance().getAllIndexes()) {
        if (type != StandardIndexes.forward()) {
          IndexHandler handler = createHandler(type);
          indexHandlers.add(handler);
          handler.updateIndices(segmentWriter);
        }
      }

      // Perform post-cleanup operations on the index handlers.
      for (IndexHandler handler : indexHandlers) {
        handler.postUpdateIndicesCleanup(segmentWriter);
      }

      // Index handler might modify the segment metadata, so we need to fetch it again
      segmentMetadata = _segmentDirectory.getSegmentMetadata();

      // Add min/max value to column metadata according to the prune mode.
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
          _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
      if (columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE) {
        ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
            new ColumnMinMaxValueGenerator(segmentMetadata, segmentWriter, columnMinMaxValueGeneratorMode);
        columnMinMaxValueGenerator.addColumnMinMaxValue();
        _segmentDirectory.reloadMetadata();
      }

      segmentWriter.save();
    }

    // Startree creation will load the segment again, so we need to close and re-open the segment writer to make sure
    // that the other required indices (e.g. forward index) are up-to-date.
    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      if (processStarTrees(indexDir, segmentOperationsThrottler)) {
        _segmentDirectory.reloadMetadata();
        segmentWriter.save();
      }
      // Create/modify/remove multi-col text index if required.
      if (processMultiColTextIndex(indexDir, segmentWriter, segmentOperationsThrottler)) {
        // NOTE: When adding new steps after this, un-comment the next line.
        //_segmentDirectory.reloadMetadata();
        segmentWriter.save();
      }
    }
  }

  private IndexHandler createHandler(IndexType<?, ?, ?> type) {
    return type.createIndexHandler(_segmentDirectory, _indexLoadingConfig.getFieldIndexConfigByColName(), _schema,
        _tableConfig);
  }

  /**
   * This method checks if there is any discrepancy between the segment and current table config and schema.
   * If so, it returns true indicating the segment needs to be reprocessed. Right now, the default columns,
   * all types of indices and column min/max values are checked against what's set in table config and schema.
   */
  public boolean needProcess()
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    if (segmentMetadata.getTotalDocs() == 0) {
      return false;
    }
    String segmentName = segmentMetadata.getName();
    try (SegmentDirectory.Reader segmentReader = _segmentDirectory.createReader()) {
      // Check if there is need to update default columns according to the schema.
      DefaultColumnHandler defaultColumnHandler =
          DefaultColumnHandlerFactory.getDefaultColumnHandler(null, segmentMetadata, _indexLoadingConfig, null);
      if (defaultColumnHandler.needUpdateDefaultColumns()) {
        LOGGER.info("Found default columns need updates in segment: {}", segmentName);
        return true;
      }
      // Check if there is need to update single-column indices, like inverted index, json index etc.
      for (IndexType<?, ?, ?> type : IndexService.getInstance().getAllIndexes()) {
        if (createHandler(type).needUpdateIndices(segmentReader)) {
          LOGGER.info("Found index type: {} needs updates in segment: {}", type, segmentName);
          return true;
        }
      }
      // Check if there is need to create/modify/remove star-trees.
      if (needProcessStarTrees()) {
        LOGGER.info("Found startree index needs updates in segment: {}", segmentName);
        return true;
      }

      // Check if there is need to create/modify/remove multi-col text index
      if (needProcessMultiColumnTextIndex()) {
        LOGGER.info("Found multi-column text index needs updates in segment: {}", segmentName);
        return true;
      }

      // Check if there is need to update column min max value.
      List<String> columnMinMaxValueUpdates = columnMinMaxValueUpdates();
      if (!columnMinMaxValueUpdates.isEmpty()) {
        LOGGER.info("Found min max values need updates for columns: {} in segment: {}", columnMinMaxValueUpdates,
            segmentName);
        return true;
      }
    }
    return false;
  }

  private List<String> columnMinMaxValueUpdates() {
    ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
        _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode == ColumnMinMaxValueGeneratorMode.NONE) {
      return Collections.emptyList();
    }
    ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
        new ColumnMinMaxValueGenerator(_segmentDirectory.getSegmentMetadata(), null, columnMinMaxValueGeneratorMode);
    return columnMinMaxValueGenerator.columnMinMaxValueUpdates();
  }

  private boolean needProcessStarTrees() {
    // Check if there is need to create/modify/remove star-trees.
    if (!_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
      return false;
    }

    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    List<StarTreeV2BuilderConfig> starTreeBuilderConfigs =
        StarTreeBuilderUtils.generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
            _indexLoadingConfig.isEnableDefaultStarTree(), segmentMetadata);
    List<StarTreeV2Metadata> starTreeMetadataList = segmentMetadata.getStarTreeV2MetadataList();
    // There are existing star-trees, but if they match the builder configs exactly,
    // then there is no need to generate the star-trees

    // We need reprocessing if existing configs are to be removed, or new configs have been added
    if (starTreeMetadataList != null) {
      return StarTreeBuilderUtils.shouldModifyExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList);
    }
    return !starTreeBuilderConfigs.isEmpty();
  }

  private boolean needProcessMultiColumnTextIndex() {
    MultiColumnTextIndexConfig newConfig = _indexLoadingConfig.getMultiColTextIndexConfig();
    MultiColumnTextMetadata oldConfig = _segmentDirectory.getSegmentMetadata().getMultiColumnTextMetadata();
    return MultiColumnTextIndexHandler.shouldModifyMultiColTextIndex(newConfig, oldConfig);
  }

  private boolean processMultiColTextIndex(File indexDir, SegmentDirectory.Writer segmentWriter,
      @Nullable SegmentOperationsThrottler segmentOperationsThrottler)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    MultiColumnTextMetadata oldConfig = segmentMetadata.getMultiColumnTextMetadata();
    MultiColumnTextIndexConfig newConfig = _indexLoadingConfig.getMultiColTextIndexConfig();
    boolean remove = false;
    boolean create = newConfig != null;

    if (oldConfig != null) {
      if (newConfig == null) {
        remove = true;
      } else {
        if (MultiColumnTextIndexHandler.shouldModifyMultiColTextIndex(newConfig, oldConfig)) {
          LOGGER.info("Change detected in multi-column text index for segment: {}", segmentName);
        } else {
          create = false;
        }
      }
    }
    if (!remove && !create) {
      LOGGER.info("No change detected in multi-column text index for segment: {}", segmentName);
      return false;
    }

    if (segmentOperationsThrottler != null) {
      segmentOperationsThrottler.getSegmentMultiColTextIndexPreprocessThrottler().acquire();
    }
    try {
      if (remove) {
        LOGGER.info("Removing multi-column text index from segment: {}", segmentName);
        removeMultiColumnTextIndex(indexDir);
      } else if (create) {
        if (oldConfig != null) {
          // Drop existing multi-column text index before creating a new one
          // TODO: check if it's possible to only add/remove select columns
          removeMultiColumnTextIndex(indexDir);
        }
        MultiColumnTextIndexHandler handler =
            new MultiColumnTextIndexHandler(_segmentDirectory, _indexLoadingConfig, newConfig);
        handler.updateIndices(segmentWriter);
        handler.postUpdateIndicesCleanup(segmentWriter);
      }
    } finally {
      if (segmentOperationsThrottler != null) {
        segmentOperationsThrottler.getSegmentMultiColTextIndexPreprocessThrottler().release();
      }
    }
    return true;
  }

  private void removeMultiColumnTextIndex(File indexDir)
      throws ConfigurationException, IOException {
    // Remove the multi-col text index metadata
    PropertiesConfiguration metadataProperties = SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    metadataProperties.subset(MultiColumnTextIndexConstants.MetadataKey.ROOT_SUBSET).clear();
    SegmentMetadataUtils.savePropertiesConfiguration(metadataProperties, indexDir);

    // Remove the index file and index map file
    File segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File textIdxDir =
        SegmentDirectoryPaths.findTextIndexIndexFile(segmentDirectory, MultiColumnTextIndexConstants.INDEX_DIR_NAME);

    if (textIdxDir != null && textIdxDir.exists()) {
      FileUtils.forceDelete(textIdxDir);
    }
    File mappingFile = new File(segmentDirectory, MultiColumnTextIndexConstants.DOCID_MAPPING_FILE_NAME);
    if (mappingFile.exists()) {
      FileUtils.forceDelete(mappingFile);
    }
  }

  private boolean processStarTrees(File indexDir,
      @Nullable SegmentOperationsThrottler segmentOperationsThrottler)
      throws Exception {
    if (!_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
      return false;
    }

    SegmentMetadataImpl segmentMetadata = _segmentDirectory.getSegmentMetadata();
    String segmentName = segmentMetadata.getName();
    List<StarTreeV2BuilderConfig> starTreeBuilderConfigs =
        StarTreeBuilderUtils.generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
            _indexLoadingConfig.isEnableDefaultStarTree(), segmentMetadata);

    boolean shouldGenerateStarTree = !starTreeBuilderConfigs.isEmpty();
    boolean shouldRemoveStarTree = false;
    List<StarTreeV2Metadata> starTreeMetadataList = segmentMetadata.getStarTreeV2MetadataList();
    if (starTreeMetadataList != null) {
      // There are existing star-trees
      if (!shouldGenerateStarTree) {
        // Newer config does not have star-trees. Delete all existing star-trees.
        shouldRemoveStarTree = true;
      } else if (StarTreeBuilderUtils.shouldModifyExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList)) {
        // Existing and newer both have star-trees, but they don't match. Rebuild the star-trees.
        LOGGER.info("Change detected in star-trees for segment: {}", segmentName);
      } else {
        // Existing star-trees match the builder configs, no need to generate the star-trees
        shouldGenerateStarTree = false;
      }
    }
    if (!shouldGenerateStarTree && !shouldRemoveStarTree) {
      return false;
    }

    if (segmentOperationsThrottler != null) {
      segmentOperationsThrottler.getSegmentStarTreePreprocessThrottler().acquire();
    }
    try {
      if (shouldRemoveStarTree) {
        // 'shouldGenerateStarTree' should be false if they need to be removed
        LOGGER.info("Removing star-trees from segment: {}", segmentName);
        StarTreeBuilderUtils.removeStarTrees(indexDir);
      } else {
        // NOTE: Always use OFF_HEAP mode on server side.
        try (MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeBuilderConfigs, indexDir,
            MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
          builder.build();
        }
      }
    } finally {
      if (segmentOperationsThrottler != null) {
        segmentOperationsThrottler.getSegmentStarTreePreprocessThrottler().release();
      }
    }
    return true;
  }

  /**
   * Remove all the existing inverted index temp files before loading segments, by looking
   * for all files in the directory and remove the ones with  '.bitmap.inv.tmp' extension.
   */
  private void removeInvertedIndexTempFiles(File indexDir) {
    File[] directoryListing = indexDir.listFiles();
    if (directoryListing == null) {
      return;
    }
    String tempFileExtension = V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION + ".tmp";
    for (File child : directoryListing) {
      if (child.getName().endsWith(tempFileExtension)) {
        FileUtils.deleteQuietly(child);
      }
    }
  }
}
