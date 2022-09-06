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
package org.apache.pinot.segment.local.loader;

import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentLoader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link SegmentDirectoryLoader} that can move segments across data dirs configured as storage tiers.
 */
@SegmentLoader(name = "tierBased")
public class TierBasedSegmentDirectoryLoader implements SegmentDirectoryLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TierBasedSegmentDirectoryLoader.class);

  /**
   * Creates and loads the {@link SegmentLocalFSDirectory} which is the default implementation of
   * {@link SegmentDirectory}
   * @param indexDir the current segment index directory
   * @param segmentLoaderContext context for instantiation of the SegmentDirectory. The target tier set in context is
   *                            used to decide which data directory to keep the segment data.
   * @return instance of {@link SegmentLocalFSDirectory}
   */
  @Override
  public SegmentDirectory load(URI indexDir, SegmentDirectoryLoaderContext segmentLoaderContext)
      throws Exception {
    File srcDir = new File(indexDir);
    String segmentName = segmentLoaderContext.getSegmentName();
    String segmentTier = segmentLoaderContext.getSegmentTier();
    File destDir = getTargetDataDir(segmentTier, segmentLoaderContext);
    if (destDir == null) {
      destDir = getDefaultDataDir(segmentLoaderContext);
      LOGGER.info("Use destDir: {} on default tier for segment: {} as no dataDir for target tier: {}", destDir,
          segmentName, segmentTier);
      segmentTier = null;
    }
    if (srcDir.equals(destDir)) {
      LOGGER.info("Keep segment: {} in current dataDir: {} on current tier: {}", segmentName, destDir, segmentTier);
    } else {
      LOGGER.info("Move segment: {} from srcDir: {} to destDir: {} on tier: {}", segmentName, srcDir, destDir,
          segmentTier);
      if (destDir.exists()) {
        LOGGER.warn("The destDir: {} exists on tier: {} and cleans it firstly", destDir, segmentTier);
        FileUtils.deleteQuietly(destDir);
      }
      FileUtils.moveDirectory(srcDir, destDir);
    }
    SegmentDirectory segmentDirectory;
    if (!destDir.exists()) {
      segmentDirectory = new SegmentLocalFSDirectory(destDir);
    } else {
      segmentDirectory = new SegmentLocalFSDirectory(destDir, ReadMode
          .valueOf(segmentLoaderContext.getSegmentDirectoryConfigs().getProperty(IndexLoadingConfig.READ_MODE_KEY)));
    }
    LOGGER.info("Created segmentDirectory object for segment: {} with dataDir: {} on tier: {}", segmentName, destDir,
        segmentTier);
    segmentDirectory.setTier(segmentTier);
    return segmentDirectory;
  }

  private File getDefaultDataDir(SegmentDirectoryLoaderContext loaderContext) {
    return new File(loaderContext.getTableDataDir(), loaderContext.getSegmentName());
  }

  private File getTargetDataDir(String segmentTier, SegmentDirectoryLoaderContext loaderContext) {
    if (segmentTier == null) {
      return null;
    }
    TableConfig tableConfig = loaderContext.getTableConfig();
    String tableNameWithType = tableConfig.getTableName();
    String segmentName = loaderContext.getSegmentName();
    try {
      String tierDataDir = TierConfigUtils.getDataDirFromTierConfig(tableNameWithType, segmentTier, tableConfig);
      File tierTableDataDir = new File(tierDataDir, tableNameWithType);
      return new File(tierTableDataDir, segmentName);
    } catch (Exception e) {
      LOGGER.warn("Failed to get dataDir for segment: {} of table: {} on tier: {} due to error: {}", segmentName,
          tableNameWithType, segmentTier, e.getMessage());
      return null;
    }
  }
}
