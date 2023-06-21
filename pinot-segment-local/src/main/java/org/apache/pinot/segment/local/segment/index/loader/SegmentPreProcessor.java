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

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGenerator;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
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

  private final URI _indexDirURI;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final Schema _schema;
  private final SegmentDirectory _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;

  public SegmentPreProcessor(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema) {
    _segmentDirectory = segmentDirectory;
    _indexDirURI = segmentDirectory.getIndexDir();
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
    _segmentMetadata = segmentDirectory.getSegmentMetadata();
  }

  @Override
  public void close()
      throws Exception {
    _segmentDirectory.close();
  }

  public void process()
      throws Exception {
    if (_segmentMetadata.getTotalDocs() == 0) {
      LOGGER.info("Skip preprocessing empty segment: {}", _segmentMetadata.getName());
      return;
    }

    // Segment processing has to be done with a local directory.
    File indexDir = new File(_indexDirURI);

    // This fixes the issue of temporary files not getting deleted after creating new inverted indexes.
    removeInvertedIndexTempFiles(indexDir);

    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      // Update default columns according to the schema.
      if (_schema != null) {
        DefaultColumnHandler defaultColumnHandler = DefaultColumnHandlerFactory
            .getDefaultColumnHandler(indexDir, _segmentMetadata, _indexLoadingConfig, _schema, segmentWriter);
        defaultColumnHandler.updateDefaultColumns();
        _segmentMetadata = new SegmentMetadataImpl(indexDir);
        _segmentDirectory.reloadMetadata();
      } else {
        LOGGER.warn("Skip creating default columns for segment: {} without schema", _segmentMetadata.getName());
      }

      // Update single-column indices, like inverted index, json index etc.
      List<IndexHandler> indexHandlers = new ArrayList<>();

      // We cannot just create all the index handlers in a random order.
      // Specifically, ForwardIndexHandler needs to be executed first. This is because it modifies the segment metadata
      // while rewriting forward index to create a dictionary. Some other handlers (like the range one) assume that
      // metadata was already been modified by ForwardIndexHandler.
      IndexHandler forwardHandler = createHandler(StandardIndexes.forward());
      indexHandlers.add(forwardHandler);
      forwardHandler.updateIndices(segmentWriter);

      // Now that ForwardIndexHandler.updateIndices has been updated, we can run all other indexes in any order

      _segmentMetadata = new SegmentMetadataImpl(indexDir);
      _segmentDirectory.reloadMetadata();

      for (IndexType<?, ?, ?> type : IndexService.getInstance().getAllIndexes()) {
        if (type != StandardIndexes.forward()) {
          IndexHandler handler = createHandler(type);
          indexHandlers.add(handler);
          handler.updateIndices(segmentWriter);
          // Other IndexHandler classes may modify the segment metadata while creating a temporary forward
          // index to generate their respective indexes from if the forward index was disabled. This new metadata is
          // needed to construct other indexes like RangeIndex.
          _segmentMetadata = _segmentDirectory.getSegmentMetadata();
        }
      }

      // Create/modify/remove star-trees if required.
      processStarTrees(indexDir);

      // Perform post-cleanup operations on the index handlers. This should be called after processing the startrees
      for (IndexHandler handler : indexHandlers) {
        handler.postUpdateIndicesCleanup(segmentWriter);
      }

      // Add min/max value to column metadata according to the prune mode.
      // For star-tree index, because it can only increase the range, so min/max value can still be used in pruner.
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
          _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
      if (columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE) {
        ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
            new ColumnMinMaxValueGenerator(_segmentMetadata, segmentWriter, columnMinMaxValueGeneratorMode);
        columnMinMaxValueGenerator.addColumnMinMaxValue();
        // NOTE: This step may modify the segment metadata. When adding new steps after this, un-comment the next line.
        // _segmentMetadata = new SegmentMetadataImpl(indexDir);
      }

      segmentWriter.save();
    }
  }

  private IndexHandler createHandler(IndexType<?, ?, ?> type) {
    return type.createIndexHandler(_segmentDirectory,
        _indexLoadingConfig.getFieldIndexConfigByColName(), _schema, _indexLoadingConfig.getTableConfig());
  }

  /**
   * This method checks if there is any discrepancy between the segment and current table config and schema.
   * If so, it returns true indicating the segment needs to be reprocessed. Right now, the default columns,
   * all types of indices and column min/max values are checked against what's set in table config and schema.
   */
  public boolean needProcess()
      throws Exception {
    if (_segmentMetadata.getTotalDocs() == 0) {
      return false;
    }
    try (SegmentDirectory.Reader segmentReader = _segmentDirectory.createReader()) {
      // Check if there is need to update default columns according to the schema.
      if (_schema != null) {
        DefaultColumnHandler defaultColumnHandler = DefaultColumnHandlerFactory
            .getDefaultColumnHandler(null, _segmentMetadata, _indexLoadingConfig, _schema, null);
        if (defaultColumnHandler.needUpdateDefaultColumns()) {
          LOGGER.info("Found default columns need updates");
          return true;
        }
      }
      // Check if there is need to update single-column indices, like inverted index, json index etc.
      for (IndexType<?, ?, ?> type : IndexService.getInstance().getAllIndexes()) {
        if (createHandler(type).needUpdateIndices(segmentReader)) {
          LOGGER.info("Found index type: {} needs updates", type);
          return true;
        }
      }
      // Check if there is need to create/modify/remove star-trees.
      if (needProcessStarTrees()) {
        LOGGER.info("Found startree index needs updates");
        return true;
      }
      // Check if there is need to update column min max value.
      if (needUpdateColumnMinMaxValue()) {
        LOGGER.info("Found min max values need updates");
        return true;
      }
    }
    return false;
  }

  private boolean needUpdateColumnMinMaxValue() {
    ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
        _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode == ColumnMinMaxValueGeneratorMode.NONE) {
      return false;
    }
    ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
        new ColumnMinMaxValueGenerator(_segmentMetadata, null, columnMinMaxValueGeneratorMode);
    return columnMinMaxValueGenerator.needAddColumnMinMaxValue();
  }

  private boolean needProcessStarTrees() {
    // Check if there is need to create/modify/remove star-trees.
    if (!_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
      return false;
    }
    List<StarTreeV2BuilderConfig> starTreeBuilderConfigs = StarTreeBuilderUtils
        .generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
            _indexLoadingConfig.isEnableDefaultStarTree(), _segmentMetadata);
    List<StarTreeV2Metadata> starTreeMetadataList = _segmentMetadata.getStarTreeV2MetadataList();
    // There are existing star-trees, but if they match the builder configs exactly,
    // then there is no need to generate the star-trees

    // We need reprocessing if existing configs are to be removed, or new configs have been added
    if (starTreeMetadataList != null) {
      return StarTreeBuilderUtils.shouldModifyExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList);
    }
    return !starTreeBuilderConfigs.isEmpty();
  }

  private void processStarTrees(File indexDir)
      throws Exception {
    // Create/modify/remove star-trees if required
    if (_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
      List<StarTreeV2BuilderConfig> starTreeBuilderConfigs = StarTreeBuilderUtils
          .generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
              _indexLoadingConfig.isEnableDefaultStarTree(), _segmentMetadata);
      boolean shouldGenerateStarTree = !starTreeBuilderConfigs.isEmpty();
      List<StarTreeV2Metadata> starTreeMetadataList = _segmentMetadata.getStarTreeV2MetadataList();
      if (starTreeMetadataList != null) {
        // There are existing star-trees
        if (StarTreeBuilderUtils.shouldModifyExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList)) {
          LOGGER.info("Change detected in star-trees for segment: {}", _segmentMetadata.getName());
          shouldGenerateStarTree = true;
        } else {
          // Existing star-trees match the builder configs, no need to generate the star-trees
          shouldGenerateStarTree = false;
        }
      }
      // Generate the star-trees if needed
      if (shouldGenerateStarTree) {
        // NOTE: Always use OFF_HEAP mode on server side.
        try (MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeBuilderConfigs, indexDir,
            MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
          builder.build();
        }
        _segmentMetadata = new SegmentMetadataImpl(indexDir);
      }
    }
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
