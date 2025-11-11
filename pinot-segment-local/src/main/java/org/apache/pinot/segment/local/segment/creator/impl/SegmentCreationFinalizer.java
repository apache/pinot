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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.MultiColumnTextIndexHandler;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive segment finalization handler that manages all post-creation segment operations.
 * This class encapsulates segment name generation, sealing, format conversion, index building,
 * and metadata persistence.
 */
public class SegmentCreationFinalizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationFinalizer.class);

  private final SegmentGeneratorConfig _config;
  private final InstanceType _instanceType;
  private final SegmentCreator _indexCreator;
  private final ColumnStatistics _timeStats;
  private final String _segmentName;

  /**
   * @param config Segment generator configuration
   * @param instanceType Instance type for metrics tracking (nullable - if null, metrics will not be tracked)
   * @param indexCreator Segment creator instance
   * @param timeStats Time column statistics (can be null)
   */
  public SegmentCreationFinalizer(SegmentGeneratorConfig config, @Nullable InstanceType instanceType,
      SegmentCreator indexCreator, @Nullable ColumnStatistics timeStats) {
    _config = config;
    _instanceType = instanceType;
    _indexCreator = indexCreator;
    _timeStats = timeStats;
    _segmentName = generateSegmentName();
  }

  /**
   * Get the generated segment name.
   *
   * @return Segment name
   */
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Finalize the segment with all post-creation operations.
   * This method handles:
   * - Sealing and closing the index creator
   * - Directory operations (move temp to final location)
   * - Format conversion (v1 to v3 if configured)
   * - Star-tree V2 index building
   * - Multi-column text index building
   * - Post-segment creation index updates
   * - CRC computation and metadata persistence
   *
   * @param currentIndexDir Current directory where indexes are built
   * @return Final segment directory
   * @throws Exception If finalization fails
   */
  public File finalizeSegment(File currentIndexDir)
      throws Exception {
    try {
      // Write the index files to disk
      _indexCreator.setSegmentName(_segmentName);
      _indexCreator.seal();
    } finally {
      _indexCreator.close();
    }
    LOGGER.info("Finished segment seal for: {}", _segmentName);

    // Delete the directory named after the segment name, if it exists
    final File outputDir = new File(_config.getOutDir());
    final File segmentOutputDir = new File(outputDir, _segmentName);
    if (segmentOutputDir.exists()) {
      FileUtils.deleteDirectory(segmentOutputDir);
    }
    // Move the temporary directory into its final location
    FileUtils.moveDirectory(currentIndexDir, segmentOutputDir);
    FileUtils.deleteQuietly(currentIndexDir);

    // Format conversion
    convertFormatIfNecessary(segmentOutputDir);

    // Build indexes if there are documents
    if (_timeStats.getTotalNumberOfEntries() > 0) {
      buildStarTreeV2IfNecessary(segmentOutputDir);
      buildMultiColumnTextIndex(segmentOutputDir);
    }

    // Update post-creation indexes
    updatePostSegmentCreationIndexes(segmentOutputDir);

    // Persist creation metadata
    persistCreationMeta(segmentOutputDir);

    LOGGER.info("Successfully finalized segment: {}", _segmentName);
    return segmentOutputDir;
  }

  /**
   * Generate segment name based on configuration and statistics.
   *
   * @return Generated segment name
   */
  private String generateSegmentName() {
    if (_timeStats != null) {
      if (_timeStats.getTotalNumberOfEntries() > 0) {
        return _config.getSegmentNameGenerator()
            .generateSegmentName(_config.getSequenceId(), _timeStats.getMinValue(), _timeStats.getMaxValue());
      } else {
        // When totalDoc is 0, check whether 'failOnEmptySegment' option is true
        Preconditions.checkArgument(!_config.isFailOnEmptySegment(),
            "Failing the empty segment creation as the option 'failOnEmptySegment' is set to: "
                + _config.isFailOnEmptySegment());
        // Generate a unique name for a segment with no rows
        long now = System.currentTimeMillis();
        return _config.getSegmentNameGenerator().generateSegmentName(_config.getSequenceId(), now, now);
      }
    } else {
      return _config.getSegmentNameGenerator().generateSegmentName(_config.getSequenceId(), null, null);
    }
  }

  // Explanation of why we are using format converter:
  // There are 3 options to correctly generate segments to v3 format
  // 1. Generate v3 directly: This is efficient but v3 index writer needs to know buffer size upfront.
  // Inverted, star and raw indexes don't have the index size upfront. This is also least flexible approach
  // if we add more indexes in the future.
  // 2. Hold data in-memory: One way to work around predeclaring sizes in (1) is to allocate "large" buffer (2GB?)
  // and hold the data in memory and write the buffer at the end. The memory requirement in this case increases linearly
  // with the number of columns. Variation of that is to mmap data to separate files...which is what we are doing here
  // 3. Another option is to generate dictionary and fwd indexes in v3 and generate inverted, star and raw indexes in
  // separate files. Then add those files to v3 index file. This leads to lot of hodgepodge code to
  // handle multiple segment formats.
  // Using converter is similar to option (2), plus it's battle-tested code. We will roll out with
  // this change to keep changes limited. Once we've migrated we can implement approach (1) with option to
  // copy for indexes for which we don't know sizes upfront.
  private void convertFormatIfNecessary(File segmentDirectory)
      throws Exception {
    SegmentVersion versionToGenerate = _config.getSegmentVersion();
    if (versionToGenerate.equals(SegmentVersion.v1)) {
      // v1 by default
      return;
    }
    SegmentFormatConverter converter =
        SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v3);
    converter.convert(segmentDirectory);
  }

  /**
   * Build star-tree V2 index if configured.
   *
   * @param indexDir Segment index directory
   * @throws Exception If star-tree index building fails
   */
  private void buildStarTreeV2IfNecessary(File indexDir)
      throws Exception {
    List<StarTreeIndexConfig> starTreeIndexConfigs = _config.getStarTreeIndexConfigs();
    boolean enableDefaultStarTree = _config.isEnableDefaultStarTree();
    if (CollectionUtils.isNotEmpty(starTreeIndexConfigs) || enableDefaultStarTree) {
      MultipleTreesBuilder.BuildMode buildMode =
          _config.isOnHeap() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
      MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeIndexConfigs, enableDefaultStarTree, indexDir,
          buildMode);
      // We don't create the builder using the try-with-resources pattern because builder.close() performs
      // some clean-up steps to roll back the star-tree index to the previous state if it exists. If this goes wrong
      // the star-tree index can be in an inconsistent state. To prevent that, when builder.close() throws an
      // exception we want to propagate that up instead of ignoring it. This can get clunky when using
      // try-with-resources as in this scenario the close() exception will be added to the suppressed exception list
      // rather than thrown as the main exception, even though the original exception thrown on build() is ignored.
      try {
        builder.build();
      } catch (Exception e) {
        String tableNameWithType = _config.getTableConfig().getTableName();
        LOGGER.error("Failed to build star-tree index for table: {}, skipping", tableNameWithType, e);
        // Track metrics only if instance type is provided
        if (_instanceType != null) {
          if (_instanceType == InstanceType.MINION) {
            MinionMetrics.get().addMeteredTableValue(tableNameWithType, MinionMeter.STAR_TREE_INDEX_BUILD_FAILURES, 1);
          } else {
            ServerMetrics.get().addMeteredTableValue(tableNameWithType, ServerMeter.STAR_TREE_INDEX_BUILD_FAILURES, 1);
          }
        }
      } finally {
        builder.close();
      }
    }
  }

  /**
   * Build multi-column text index if configured.
   *
   * @param segmentOutputDir Segment output directory
   * @throws Exception If multi-column text index building fails
   */
  private void buildMultiColumnTextIndex(File segmentOutputDir)
      throws Exception {
    if (_config.getMultiColumnTextIndexConfig() != null) {
      PinotConfiguration segmentDirectoryConfigs =
          new PinotConfiguration(Map.of(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap));

      TableConfig tableConfig = _config.getTableConfig();
      Schema schema = _config.getSchema();
      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder()
              .setTableConfig(tableConfig)
              .setSchema(schema)
              .setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
              .build();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig, schema);

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(segmentOutputDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        MultiColumnTextIndexHandler handler = new MultiColumnTextIndexHandler(segmentDirectory, indexLoadingConfig,
            _config.getMultiColumnTextIndexConfig());
        handler.updateIndices(segmentWriter);
        handler.postUpdateIndicesCleanup(segmentWriter);
      }
    }
  }

  /**
   * Update indexes that are created post-segment creation.
   *
   * @param indexDir Segment index directory
   * @throws Exception If index update fails
   */
  private void updatePostSegmentCreationIndexes(File indexDir)
      throws Exception {
    Set<IndexType> postSegCreationIndexes = IndexService.getInstance().getAllIndexes().stream()
        .filter(indexType -> indexType.getIndexBuildLifecycle() == IndexType.BuildLifecycle.POST_SEGMENT_CREATION)
        .collect(Collectors.toSet());

    if (!postSegCreationIndexes.isEmpty()) {
      // Build other indexes
      Map<String, Object> props = new HashMap<>();
      props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap);
      PinotConfiguration segmentDirectoryConfigs = new PinotConfiguration(props);

      TableConfig tableConfig = _config.getTableConfig();
      Schema schema = _config.getSchema();
      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder()
              .setTableConfig(tableConfig)
              .setSchema(schema)
              .setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
              .build();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig, schema);

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(indexDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        for (IndexType indexType : postSegCreationIndexes) {
          IndexHandler handler =
              indexType.createIndexHandler(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(), schema,
                  tableConfig);
          handler.updateIndices(segmentWriter);
        }
      }
    }
  }

  /**
   * Compute CRC and creation time, and persist to segment metadata file.
   *
   * @param indexDir Segment index directory
   * @throws IOException If writing metadata fails
   */
  private void persistCreationMeta(File indexDir)
      throws IOException {
    long crc = CrcUtils.forAllFilesInFolder(indexDir).computeCrc();
    long creationTime;
    String creationTimeInConfig = _config.getCreationTime();
    if (creationTimeInConfig != null) {
      try {
        creationTime = Long.parseLong(creationTimeInConfig);
      } catch (Exception e) {
        LOGGER.error("Caught exception while parsing creation time in config, use current time as creation time");
        creationTime = System.currentTimeMillis();
      }
    } else {
      creationTime = System.currentTimeMillis();
    }
    File segmentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File creationMetaFile = new File(segmentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(creationMetaFile))) {
      output.writeLong(crc);
      output.writeLong(creationTime);
    }
  }
}
