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
package org.apache.pinot.core.segment.index.loader;

import java.io.File;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.loader.bloomfilter.BloomFilterHandler;
import org.apache.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGenerator;
import org.apache.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import org.apache.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import org.apache.pinot.core.segment.index.loader.invertedindex.H3IndexHandler;
import org.apache.pinot.core.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.core.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.core.segment.index.loader.invertedindex.LuceneFSTIndexHandler;
import org.apache.pinot.core.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.core.segment.index.loader.invertedindex.TextIndexHandler;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.core.startree.StarTreeBuilderUtils;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.core.startree.v2.builder.StarTreeV2BuilderConfig;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
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

  private final File _indexDir;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final Schema _schema;
  private final SegmentDirectory _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;

  public SegmentPreProcessor(File indexDir, IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
      throws Exception {
    _indexDir = indexDir;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
    _segmentMetadata = new SegmentMetadataImpl(indexDir);

    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    _segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, _segmentMetadata, ReadMode.mmap);
  }

  public void process()
      throws Exception {
    if (_segmentMetadata.getTotalDocs() == 0) {
      LOGGER.info("Skip preprocessing empty segment: {}", _segmentMetadata.getName());
      return;
    }
    // Remove all the existing inverted index temp files before loading segments.
    // NOTE: This step fixes the issue of temporary files not getting deleted after creating new inverted indexes.
    // In this, we look for all files in the directory and remove the ones with  '.bitmap.inv.tmp' extension.
    File[] directoryListing = _indexDir.listFiles();
    String tempFileExtension = V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION + ".tmp";
    if (directoryListing != null) {
      for (File child : directoryListing) {
        if (child.getName().endsWith(tempFileExtension)) {
          FileUtils.deleteQuietly(child);
        }
      }
    }

    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      // Update default columns according to the schema.
      if (_schema != null) {
        DefaultColumnHandler defaultColumnHandler = DefaultColumnHandlerFactory
            .getDefaultColumnHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, _schema, segmentWriter);
        defaultColumnHandler.updateDefaultColumns();
        _segmentMetadata = new SegmentMetadataImpl(_indexDir);
        _segmentDirectory.reloadMetadata();
      } else {
        LOGGER.warn("Skip creating default columns for segment: {} without schema", _segmentMetadata.getName());
      }

      // Create column inverted indices according to the index config.
      InvertedIndexHandler invertedIndexHandler =
          new InvertedIndexHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, segmentWriter);
      invertedIndexHandler.createInvertedIndices();

      // Create column range indices according to the index config.
      RangeIndexHandler rangeIndexHandler =
          new RangeIndexHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, segmentWriter);
      rangeIndexHandler.createRangeIndices();

      // Create text indices according to the index config.
      Set<String> textIndexColumns = _indexLoadingConfig.getTextIndexColumns();
      if (!textIndexColumns.isEmpty()) {
        TextIndexHandler textIndexHandler =
            new TextIndexHandler(_indexDir, _segmentMetadata, textIndexColumns, segmentWriter);
        textIndexHandler.createTextIndexesOnSegmentLoad();
      }

      Set<String> fstIndexColumns = _indexLoadingConfig.getFSTIndexColumns();
      if (!fstIndexColumns.isEmpty()) {
        LuceneFSTIndexHandler luceneFSTIndexHandler =
            new LuceneFSTIndexHandler(_indexDir, _segmentMetadata, fstIndexColumns, segmentWriter);
        luceneFSTIndexHandler.createFSTIndexesOnSegmentLoad();
      }

      // Create json indices according to the index config.
      JsonIndexHandler jsonIndexHandler =
          new JsonIndexHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, segmentWriter);
      jsonIndexHandler.createJsonIndices();

      // Create H3 indices according to the index config.
      if (_indexLoadingConfig.getH3IndexConfigs() != null) {
        H3IndexHandler h3IndexHandler =
            new H3IndexHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, segmentWriter);
        h3IndexHandler.createH3Indices();
      }

      // Create bloom filter if required
      BloomFilterHandler bloomFilterHandler =
          new BloomFilterHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, segmentWriter);
      bloomFilterHandler.createBloomFilters();

      // Create/modify/remove star-trees if required
      if (_indexLoadingConfig.isEnableDynamicStarTreeCreation()) {
        List<StarTreeV2BuilderConfig> starTreeBuilderConfigs = StarTreeBuilderUtils
            .generateBuilderConfigs(_indexLoadingConfig.getStarTreeIndexConfigs(),
                _indexLoadingConfig.isEnableDefaultStarTree(), _segmentMetadata);
        boolean shouldGenerateStarTree = !starTreeBuilderConfigs.isEmpty();
        List<StarTreeV2Metadata> starTreeMetadataList = _segmentMetadata.getStarTreeV2MetadataList();
        if (starTreeMetadataList != null) {
          // There are existing star-trees
          if (StarTreeUtils.shouldRemoveExistingStarTrees(starTreeBuilderConfigs, starTreeMetadataList)) {
            // Remove the existing star-trees
            LOGGER.info("Removing star-trees from segment: {}", _segmentMetadata.getName());
            StarTreeUtils.removeStarTrees(_indexDir);
            _segmentMetadata = new SegmentMetadataImpl(_indexDir);
          } else {
            // Existing star-trees match the builder configs, no need to generate the star-trees
            shouldGenerateStarTree = false;
          }
        }
        // Generate the star-trees if needed
        if (shouldGenerateStarTree) {
          // NOTE: Always use OFF_HEAP mode on server side.
          try (MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeBuilderConfigs, _indexDir,
              MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
            builder.build();
          }
          _segmentMetadata = new SegmentMetadataImpl(_indexDir);
        }
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
//        _segmentMetadata = new SegmentMetadataImpl(_indexDir);
      }

      segmentWriter.save();
    }
  }

  @Override
  public void close()
      throws Exception {
    _segmentDirectory.close();
  }
}
