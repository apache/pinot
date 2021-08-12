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
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
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
    Preconditions.checkState(!_metadataProperties.containsKey(MetadataKey.STAR_TREE_COUNT), "Star-tree already exists");
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

  /**
   * Builds the star-trees.
   */
  public void build()
      throws Exception {
    long startTime = System.currentTimeMillis();
    int numStarTrees = _builderConfigs.size();
    LOGGER.info("Starting building {} star-trees with configs: {} using {} builder", numStarTrees, _builderConfigs,
        _buildMode);

    try (StarTreeIndexCombiner indexCombiner = new StarTreeIndexCombiner(
        new File(_segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME))) {
      File starTreeIndexDir = new File(_segmentDirectory, StarTreeV2Constants.STAR_TREE_TEMP_DIR);
      FileUtils.forceMkdir(starTreeIndexDir);
      _metadataProperties.addProperty(MetadataKey.STAR_TREE_COUNT, numStarTrees);
      List<Map<IndexKey, IndexValue>> indexMaps = new ArrayList<>(numStarTrees);

      // Build all star-trees
      for (int i = 0; i < numStarTrees; i++) {
        StarTreeV2BuilderConfig builderConfig = _builderConfigs.get(i);
        Configuration metadataProperties = _metadataProperties.subset(MetadataKey.getStarTreePrefix(i));
        try (SingleTreeBuilder singleTreeBuilder = getSingleTreeBuilder(builderConfig, starTreeIndexDir, _segment,
            metadataProperties, _buildMode)) {
          singleTreeBuilder.build();
        }
        indexMaps.add(indexCombiner.combine(builderConfig, starTreeIndexDir));
      }

      // Save the metadata and index maps to the disk
      // Commons Configuration 1.10 does not support file path containing '%'. 
      // Explicitly providing the output stream for the file bypasses the problem.       
      try (FileOutputStream fileOutputStream = new FileOutputStream(_metadataProperties.getFile())) {
        _metadataProperties.save(fileOutputStream);
      }
      StarTreeIndexMapUtils
          .storeToFile(indexMaps, new File(_segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME));
      FileUtils.forceDelete(starTreeIndexDir);
    }

    LOGGER.info("Finished building {} star-trees in {}ms", numStarTrees, System.currentTimeMillis() - startTime);
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
    _segment.destroy();
  }
}
