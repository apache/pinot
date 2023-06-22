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
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
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
  private final File _indexDir;
  private final PropertiesConfiguration _metadataProperties;
  private final ImmutableSegment _segment;
  private final SeparatedStarTreesMetadata _existingStarTrees;

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
    _indexDir = indexDir;
    _metadataProperties =
        CommonsConfigurationUtils.fromFile(new File(_segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    _existingStarTrees = getExistingStarTrees();
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
    _indexDir = indexDir;
    _segmentDirectory = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    _metadataProperties =
        CommonsConfigurationUtils.fromFile(new File(_segmentDirectory, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    _existingStarTrees = getExistingStarTrees();
    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    try {
      _builderConfigs = StarTreeBuilderUtils
          .generateBuilderConfigs(indexConfigs, enableDefaultStarTree, _segment.getSegmentMetadata());
    } catch (Exception e) {
      _segment.destroy();
      throw e;
    }
  }

  private SeparatedStarTreesMetadata getExistingStarTrees()
      throws Exception {
    try {
      if (_metadataProperties.containsKey(MetadataKey.STAR_TREE_COUNT)) {
        // Extract existing startrees
        // clean star-tree related files and configs once all the star-trees are separated and extracted
        SeparatedStarTreesMetadata existingStarTrees = extractStarTrees();
        StarTreeBuilderUtils.removeStarTrees(_indexDir);
        _metadataProperties.refresh();
        return existingStarTrees;
      }
      return null;
    } catch (Exception e) {
      try {
        SeparatedStarTreesMetadata.cleanOutputDirectory(_segmentDirectory);
      } catch (Exception e1) {
        LOGGER.warn(e1.getMessage(), e1);
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
        if (handleExistingStarTreeAddition(starTreeIndexDir, metadataProperties, builderConfig)) {
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
   * Extracts the individual star-trees from the combined index file and returns {@link SeparatedStarTreesMetadata}
   * which contains the information about the output directory where the extracted star-trees are located and also
   * has the builder configs of each of the star-tree.
   */
  private SeparatedStarTreesMetadata extractStarTrees()
      throws Exception {
    SeparatedStarTreesMetadata metadata = new SeparatedStarTreesMetadata(_segmentDirectory);
    File indexFile = new File(_segmentDirectory, StarTreeV2Constants.INDEX_FILE_NAME);
    File indexMapFile = new File(_segmentDirectory, StarTreeV2Constants.INDEX_MAP_FILE_NAME);
    try (StarTreeIndexSeparator separator =
        new StarTreeIndexSeparator(indexMapFile, indexFile,
            _metadataProperties.getInt(MetadataKey.STAR_TREE_COUNT))) {
      separator.separate(metadata.getOutputDirectory());
      metadata.setBuilderConfigList(separator.extractBuilderConfigs(_metadataProperties));
      metadata.setTotalDocsList(separator.extractTotalDocsList(_metadataProperties));
    }
    return metadata;
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
    int builderConfigIndex = -1;
    if (_existingStarTrees != null) {
      builderConfigIndex = _existingStarTrees.getIndexOf(builderConfig);
    }
    if (builderConfigIndex == -1) {
      return false;
    }
    File existingStarTreeIndexSubDir = _existingStarTrees.getSubDirectory(builderConfigIndex);
    for (File file : FileUtils.listFiles(existingStarTreeIndexSubDir, null, false)) {
      FileUtils.moveFileToDirectory(file, starTreeIndexDir, false);
    }
    metadataProperties.setProperty(MetadataKey.TOTAL_DOCS, _existingStarTrees.getTotalDocs(builderConfigIndex));
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
    try {
      if (_existingStarTrees != null) {
        _existingStarTrees.cleanOutputDirectory();
      }
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
    }
    _segment.destroy();
  }

  private static class SeparatedStarTreesMetadata {
    private final File _outputDirectory;
    private List<StarTreeV2BuilderConfig> _builderConfigList = new ArrayList<>();
    private List<Integer> _totalDocsList = new ArrayList<>();

    public SeparatedStarTreesMetadata(File segmentDirectory)
        throws IOException {
      _outputDirectory = new File(segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR);
      FileUtils.forceMkdir(_outputDirectory);
    }

    public static void cleanOutputDirectory(File segmentDirectory)
        throws IOException {
      File outputDirectory = new File(segmentDirectory, StarTreeV2Constants.EXISTING_STAR_TREE_TEMP_DIR);
      FileUtils.forceDelete(outputDirectory);
    }

    public File getOutputDirectory() {
      return _outputDirectory;
    }

    public List<StarTreeV2BuilderConfig> getBuilderConfigList() {
      return _builderConfigList;
    }

    public void setBuilderConfigList(List<StarTreeV2BuilderConfig> builderConfigList) {
      _builderConfigList = builderConfigList;
    }

    public List<Integer> getTotalDocsList() {
      return _totalDocsList;
    }

    public void setTotalDocsList(List<Integer> totalDocsList) {
      _totalDocsList = totalDocsList;
    }

    public File getSubDirectory(int index) {
      return new File(_outputDirectory, StarTreeIndexSeparator.STARTREE_SEPARATOR_PREFIX + index);
    }

    public boolean containsTree(StarTreeV2BuilderConfig builderConfig) {
      return _builderConfigList.contains(builderConfig);
    }

    public int getIndexOf(StarTreeV2BuilderConfig builderConfig) {
      return _builderConfigList.indexOf(builderConfig);
    }

    public int getTotalDocs(int index) {
      return _totalDocsList.get(index);
    }

    public void cleanOutputDirectory()
        throws IOException {
      FileUtils.forceDelete(_outputDirectory);
    }
  }
}
