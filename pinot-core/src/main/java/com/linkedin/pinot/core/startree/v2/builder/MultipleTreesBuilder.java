/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree.v2.builder;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.startree.v2.store.StarTreeIndexMapUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.core.startree.v2.StarTreeV2Constants.*;
import static com.linkedin.pinot.core.startree.v2.store.StarTreeIndexMapUtils.*;


/**
 * The {@code MultipleTreesBuilder} class is the top level star-tree builder that takes a list of
 * {@link StarTreeV2BuilderConfig}s and builds multiple star-trees with the given {@link BuildMode} ({@code ON_HEAP} or
 * {@code OFF_HEAP}).
 * <p>The indexes for all star-trees will be stored in a single index file, and there will be an extra index map file to
 * mark the offset and size of each index in the index file.
 */
public class MultipleTreesBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultipleTreesBuilder.class);

  private final List<StarTreeV2BuilderConfig> _builderConfigs;
  private final ImmutableSegment _segment;
  private final File _segmentDirectory;
  private final PropertiesConfiguration _metadataProperties;
  private final BuildMode _buildMode;

  public enum BuildMode {
    ON_HEAP, OFF_HEAP
  }

  /**
   * Constructor for the multiple star-trees builder.
   *
   * @param builderConfigs List of builder configs
   * @param indexDir Index directory
   * @param buildMode Build mode (ON_HEAP or OFF_HEAP)
   * @throws Exception
   */
  public MultipleTreesBuilder(List<StarTreeV2BuilderConfig> builderConfigs, File indexDir, BuildMode buildMode)
      throws Exception {
    _builderConfigs = builderConfigs;
    _segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    _metadataProperties =
        new PropertiesConfiguration(new File(_segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    Preconditions.checkState(
        !_metadataProperties.getBoolean(V1Constants.MetadataKeys.StarTree.STAR_TREE_ENABLED, false),
        "Star-tree already exists");
    Preconditions.checkState(!_metadataProperties.containsKey(MetadataKey.STAR_TREE_COUNT),
        "Star-tree v2 already exists");
    _buildMode = buildMode;
  }

  /**
   * Builds the star-trees.
   */
  public void build() throws Exception {
    long startTime = System.currentTimeMillis();
    int numStarTrees = _builderConfigs.size();
    LOGGER.info("Start building {} star-trees with configs: {} using {} builder", numStarTrees, _builderConfigs,
        _buildMode);

    try (
        StarTreeIndexCombiner indexCombiner = new StarTreeIndexCombiner(new File(_segmentDirectory, INDEX_FILE_NAME))) {
      File starTreeIndexDir = new File(_segmentDirectory, STAR_TREE_TEMP_DIR);
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
      _metadataProperties.save();
      StarTreeIndexMapUtils.storeToFile(indexMaps, new File(_segmentDirectory, INDEX_MAP_FILE_NAME));
      FileUtils.forceDelete(starTreeIndexDir);
    }

    LOGGER.info("Finish building {} star-trees in {}ms", numStarTrees, System.currentTimeMillis() - startTime);
  }

  private static SingleTreeBuilder getSingleTreeBuilder(StarTreeV2BuilderConfig builderConfig, File outputDir,
      ImmutableSegment segment, Configuration metadataProperties, BuildMode buildMode) throws FileNotFoundException {
    if (buildMode == BuildMode.ON_HEAP) {
      return new OnHeapSingleTreeBuilder(builderConfig, outputDir, segment, metadataProperties);
    } else {
      return new OffHeapSingleTreeBuilder(builderConfig, outputDir, segment, metadataProperties);
    }
  }
}
