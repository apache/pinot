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
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
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
  private static final String SEGMENT_TIER_TRACK_FILE_SUFFIX = ".tier";

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
    String segmentName = segmentLoaderContext.getSegmentName();
    File srcDir = new File(indexDir);
    // The srcDir should exist for most cases, otherwise use data dir on the last known tier as tracked by server.
    if (!srcDir.exists()) {
      String[] lastTierPath = getSegmentTierPersistedLocally(segmentName, segmentLoaderContext);
      String lastTierName = TierConfigUtils.normalizeTierName(lastTierPath[0]);
      LOGGER.info("The srcDir: {} does not exist for segment: {}. Try data dir on last known tier: {}", srcDir,
          segmentName, lastTierName);
      File lastDataDir = lastTierPath[1] != null ? new File(lastTierPath[1]) : getDefaultDataDir(segmentLoaderContext);
      if (lastDataDir.equals(srcDir)) {
        LOGGER.info("The dataDir: {} on last known tier: {} is same as srcDir", lastDataDir, lastTierName);
      } else {
        LOGGER.warn("Use dataDir: {} on last known tier: {} as the srcDir", lastDataDir, lastTierName);
        srcDir = lastDataDir;
      }
    }
    String targetTier = segmentLoaderContext.getSegmentTier();
    File destDir = getSegmentDataDir(targetTier, segmentLoaderContext);
    // Use default tier as the target tier if the provided destDir is not found.
    if (destDir == null) {
      if (targetTier != null) {
        LOGGER.info("No dataDir defined for targetTier: {}", TierConfigUtils.normalizeTierName(targetTier));
      }
      destDir = getDefaultDataDir(segmentLoaderContext);
      LOGGER.info("Use destDir: {} on default tier for segment: {}", destDir, segmentName);
      targetTier = null;
    }
    String targetTierName = TierConfigUtils.normalizeTierName(targetTier);
    if (srcDir.equals(destDir)) {
      LOGGER.info("Keep segment: {} in current dataDir: {} on currentTier: {}", segmentName, destDir, targetTierName);
    } else {
      LOGGER.info("Move segment: {} from srcDir: {} to destDir: {} on targetTier: {}", segmentName, srcDir, destDir,
          targetTierName);
      if (destDir.exists()) {
        LOGGER.warn("The destDir: {} exists on targetTier: {} and cleans it firstly", destDir, targetTierName);
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
    LOGGER.info("Created segmentDirectory object for segment: {} with dataDir: {} on targetTier: {}", segmentName,
        destDir, targetTierName);
    // Track current tier in SegmentDirectory object and also persist it in a file in the segment dir on default tier.
    segmentDirectory.setTier(targetTier);
    persistSegmentTierLocally(segmentName, targetTier, destDir.getAbsolutePath(), segmentLoaderContext);
    return segmentDirectory;
  }

  /**
   * Delete segment data on the last known tier as tracked in the tier track file.
   */
  @Override
  public void delete(SegmentDirectoryLoaderContext segmentLoaderContext)
      throws Exception {
    String segmentName = segmentLoaderContext.getSegmentName();
    String[] lastTierPath = getSegmentTierPersistedLocally(segmentName, segmentLoaderContext);
    File lastDataDir = lastTierPath[1] != null ? new File(lastTierPath[1]) : getDefaultDataDir(segmentLoaderContext);
    if (lastDataDir.exists()) {
      FileUtils.deleteQuietly(lastDataDir);
      LOGGER.info("Deleted segment directory {} on last known tier: {}", lastDataDir,
          TierConfigUtils.normalizeTierName(lastTierPath[0]));
    }
    deleteSegmentTierPersistedLocally(segmentName, segmentLoaderContext);
  }

  // Note that there is no need to synchronize the r/w on the segment tier track file, as the whole load() method is
  // called while holding a segmentLock, so at any time, only one thread is accessing the track file for a segment.
  private void persistSegmentTierLocally(String segmentName, String segmentTier, String segmentPath,
      SegmentDirectoryLoaderContext loaderContext)
      throws IOException {
    File trackFile = new File(loaderContext.getTableDataDir(), segmentName + SEGMENT_TIER_TRACK_FILE_SUFFIX);
    if (segmentTier != null) {
      LOGGER.info("Persist segment tier: {} and path: {} in tier track file: {}", segmentTier, segmentPath, trackFile);
      // This assumes that newline is not part of tier name or data path.
      FileUtils.writeLines(trackFile, StandardCharsets.UTF_8.name(), Arrays.asList(segmentTier, segmentPath));
    } else {
      LOGGER.info("Delete tier track file: {} for using default segment tier", trackFile);
      FileUtils.deleteQuietly(trackFile);
    }
  }

  private String[] getSegmentTierPersistedLocally(String segmentName, SegmentDirectoryLoaderContext loaderContext)
      throws IOException {
    File trackFile = new File(loaderContext.getTableDataDir(), segmentName + SEGMENT_TIER_TRACK_FILE_SUFFIX);
    if (trackFile.exists() && trackFile.length() > 0) {
      List<String> tierPath = FileUtils.readLines(trackFile, StandardCharsets.UTF_8);
      if (tierPath.size() == 2) {
        LOGGER.info("Got segment tier: {} and path: {} from tier track file: {}", tierPath.get(0), tierPath.get(1),
            trackFile);
        return tierPath.toArray(new String[2]);
      } else {
        LOGGER.warn("Got lines: {} from tier track file: {} but not as expected", tierPath, trackFile);
      }
    } else {
      LOGGER.info("No tier track file: {} so using default segment tier", trackFile);
    }
    return new String[2];
  }

  private void deleteSegmentTierPersistedLocally(String segmentName, SegmentDirectoryLoaderContext loaderContext) {
    File trackFile = new File(loaderContext.getTableDataDir(), segmentName + SEGMENT_TIER_TRACK_FILE_SUFFIX);
    LOGGER.info("Delete tier track file: {}", trackFile);
    FileUtils.deleteQuietly(trackFile);
  }

  private File getSegmentDataDirOrDefault(String segmentTier, SegmentDirectoryLoaderContext loaderContext) {
    File dataDir = getSegmentDataDir(segmentTier, loaderContext);
    return dataDir != null ? dataDir : getDefaultDataDir(loaderContext);
  }

  private File getDefaultDataDir(SegmentDirectoryLoaderContext loaderContext) {
    return new File(loaderContext.getTableDataDir(), loaderContext.getSegmentName());
  }

  private File getSegmentDataDir(String segmentTier, SegmentDirectoryLoaderContext loaderContext) {
    if (segmentTier == null) {
      return null;
    }
    TableConfig tableConfig = loaderContext.getTableConfig();
    String tableNameWithType = tableConfig.getTableName();
    String segmentName = loaderContext.getSegmentName();
    try {
      String tierDataDir = TierConfigUtils.getDataDirForTier(tableConfig, segmentTier);
      File tierTableDataDir = new File(tierDataDir, tableNameWithType);
      return new File(tierTableDataDir, segmentName);
    } catch (Exception e) {
      LOGGER.warn("Failed to get dataDir for segment: {} of table: {} on tier: {} due to error: {}", segmentName,
          tableNameWithType, segmentTier, e.getMessage());
      return null;
    }
  }
}
