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
package org.apache.pinot.segment.local.startree.v2.builder;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.startree.StarTreeBuilderUtils;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils.IndexKey;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexMapUtils.IndexValue;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants.MetadataKey;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code MultipleTreesBuilder} class is the top level star-tree builder that takes a list of
 * {@link StarTreeIndexConfig}s and a boolean flag for the default star-tree, and builds multiple star-trees with the
 * given {@link BuildMode} ({@code ON_HEAP} or {@code OFF_HEAP}).
 * <p>The indexes for all star-trees will be stored in a single index file, and there will be an extra index map file to
 * mark the offset and size of each index in the index file.
 */
public class MultipleTreesBuilder implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultipleTreesBuilder.class);

  private final List<StarTreeV2BuilderConfig> _builderConfigs;
  private final BuildMode _buildMode;
  private final File _segmentDirectory;
  private final PropertiesConfiguration _metadataProperties;
  private final ImmutableSegment _segment;
  private StarTreeIndexSeparator _separator;
  private File _separatorTempDir;

  public enum BuildMode {
    ON_HEAP, OFF_HEAP
  }

  /**
   * Constructor for the multiple star-trees builder.
   *
   * @param builderConfigs List of builder configs (should already be deduplicated)
   * @param indexDir Index directory
   * @param buildMode Build mode (ON_HEAP or OFF_HEAP)
   */
  public MultipleTreesBuilder(List<StarTreeV2BuilderConfig> builderConfigs, File indexDir, BuildMode buildMode)
      throws Exception {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(builderConfigs), "Must provide star-tree builder configs");
    _builderConfigs = builderConfigs;
    _buildMode = buildMode;
    _segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    _metadataProperties =
        CommonsConfigurationUtils.fromFile(new File(_segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    _separator = getSeparator();
    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
  }

  /**
   * Constructor for the multiple star-trees builder.
   *
   * @param indexConfigs List of index configs
   * @param enableDefaultStarTree Whether to enable the default star-tree
   * @param indexDir Index directory
   * @param buildMode Build mode (ON_HEAP or OFF_HEAP)
   */
  public MultipleTreesBuilder(@Nullable List<StarTreeIndexConfig> indexConfigs, boolean enableDefaultStarTree,
      File indexDir, BuildMode buildMode)
      throws Exception {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(indexConfigs) || enableDefaultStarTree,
        "Must provide star-tree index configs or enable default star-tree");
    _buildMode = buildMode;
    _segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    _metadataProperties =
        CommonsConfigurationUtils.fromFile(new File(_segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    Preconditions.checkState(!_metadataProperties.containsKey(MetadataKey.STAR_TREE_COUNT), "Star-tree already exists");
    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      _builderConfigs = StarTreeBuilderUtils
          .generateBuilderConfigs(indexConfigs, enableDefaultStarTree, _segment.getSegmentMetadata());
    } catch (Exception e) {
      _segment.destroy();
      throw e;
    }
  }

  @Nullable
  private StarTreeIndexSeparator getSeparator()
      throws Exception {
    if (!_metadataProperties.containsKey(MetadataKey.STAR_TREE_COUNT)) {
      return null;
    }
    try {
      _separatorTempDir = new File(_segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR);
      FileUtils.forceMkdir(_separatorTempDir);
      FileUtils.moveFileToDirectory(
          new File(_segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME), _separatorTempDir, false);
      FileUtils.moveFileToDirectory(
          new File(_segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME), _separatorTempDir, false);
      StarTreeIndexSeparator separator = new StarTreeIndexSeparator(
              new File(_separatorTempDir, StarTreeV2Constants.INDEX_MAP_FILE_NAME),
              new File(_separatorTempDir, StarTreeV2Constants.INDEX_FILE_NAME), _metadataProperties);
      _metadataProperties.subset(StarTreeV2Constants.MetadataKey.STAR_TREE_SUBSET).clear();
      SegmentMetadataUtils.savePropertiesConfiguration(_metadataProperties);
      return separator;
    } catch (Exception e) {
      try {
        FileUtils.forceDelete(_separatorTempDir);
      } catch (Exception e1) {
        LOGGER.warn("Caught exception while deleting the separator tmp directory: {}",
                _separatorTempDir.getAbsolutePath());
      }
      throw e;
    }
  }

  /**
   * Builds the star-trees.
   */
  public void build()
      throws Exception {
    long startTime = System.currentTimeMillis();
    int numStarTrees = _builderConfigs.size();
    int reusedStarTrees = 0;
    LOGGER.info("Starting building {} star-trees with configs: {} using {} builder", numStarTrees, _builderConfigs,
        _buildMode);

    try (StarTreeIndexCombiner indexCombiner = new StarTreeIndexCombiner(
        new File(_segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME))) {
      File starTreeIndexDir = new File(_segmentDirectory, StarTreeV2Constants.STAR_TREE_TEMP_DIR);
      FileUtils.forceMkdir(starTreeIndexDir);
      _metadataProperties.addProperty(MetadataKey.STAR_TREE_COUNT, numStarTrees);
      List<List<Pair<IndexKey, IndexValue>>> indexMaps = new ArrayList<>(numStarTrees);

      // Build all star-trees
      for (int i = 0; i < numStarTrees; i++) {
        StarTreeV2BuilderConfig builderConfig = _builderConfigs.get(i);
        Configuration metadataProperties = _metadataProperties.subset(MetadataKey.getStarTreePrefix(i));
        if (_separator != null && handleExistingStarTreeAddition(starTreeIndexDir, metadataProperties, builderConfig)) {
          // Used existing tree
          LOGGER.info("Reused existing star-tree: {}", builderConfig.toString());
          reusedStarTrees++;
        } else {
          try (SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(builderConfig, starTreeIndexDir, _segment,
              metadataProperties, _buildMode)) {
            singleTreeBuilder.build();
          }
        }
        indexMaps.add(indexCombiner.combine(builderConfig, starTreeIndexDir));
      }

      // Save the metadata and index maps to the disk
      SegmentMetadataUtils.savePropertiesConfiguration(_metadataProperties);
      StarTreeIndexMapUtils
          .storeToFile(indexMaps, new File(_segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME));
      FileUtils.forceDelete(starTreeIndexDir);
    }

    LOGGER.info("Finished building {} star-trees ({} reused) in {}ms", numStarTrees, reusedStarTrees,
        System.currentTimeMillis() - startTime);
  }

  /**
   * Helper utility to move the individual star-tree files to the {@param starTreeIndexDir} from where it will be picked
   * by the combiner to merge them into the single star-tree index file. The method also takes care of updating the
   * {@param metadataProperties} for the star-tree.
   * Returns {@code false} if the star-tree is not present in the existing star-trees, otherwise returns {@code true}
   * upon successful transfer completion
   */
  private boolean handleExistingStarTreeAddition(File starTreeIndexDir, Configuration metadataProperties,
      StarTreeV2BuilderConfig builderConfig)
      throws IOException {
    int totalDocs = _separator.separate(starTreeIndexDir, builderConfig);
    if (totalDocs == -1) {
      return false;
    }
    metadataProperties.setProperty(MetadataKey.TOTAL_DOCS, totalDocs);
    metadataProperties.setProperty(MetadataKey.DIMENSIONS_SPLIT_ORDER, builderConfig.getDimensionsSplitOrder());
    metadataProperties.setProperty(MetadataKey.FUNCTION_COLUMN_PAIRS, builderConfig.getFunctionColumnPairs());
    metadataProperties.setProperty(MetadataKey.MAX_LEAF_RECORDS, builderConfig.getMaxLeafRecords());
    metadataProperties.setProperty(MetadataKey.SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS,
        builderConfig.getSkipStarNodeCreationForDimensions());
    return true;
  }

  private static SingleTreeBuilder getSingleTreeBuilder(StarTreeV2BuilderConfig builderConfig, File outputDir,
      ImmutableSegment segment, Configuration metadataProperties, BuildMode buildMode)
      throws FileNotFoundException {
    if (buildMode == BuildMode.ON_HEAP) {
      return new OnHeapSingleTreeBuilder(builderConfig, outputDir, segment, metadataProperties);
    } else {
      return new OffHeapSingleTreeBuilder(builderConfig, outputDir, segment, metadataProperties);
    }
  }

  @Override
  public void close() {
    if (_separatorTempDir != null) {
      try {
        FileUtils.forceDelete(_separatorTempDir);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while deleting the separator tmp directory: {}",
                _separatorTempDir.getAbsolutePath());
      }
    }
    _segment.destroy();
  }
}
