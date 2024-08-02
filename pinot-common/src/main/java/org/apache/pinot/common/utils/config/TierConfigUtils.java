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
package org.apache.pinot.common.utils.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.tier.FixedTierSegmentSelector;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.tier.TimeBasedTierSegmentSelector;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util methods for TierConfig
 */
public final class TierConfigUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TierConfigUtils.class);

  private TierConfigUtils() {
  }

  /**
   * Returns whether relocation of segments to tiers has been enabled for this table
   */
  public static boolean shouldRelocateToTiers(TableConfig tableConfig) {
    return CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList());
  }

  public static String normalizeTierName(String tierName) {
    return tierName == null ? "default" : tierName;
  }

  /**
   * Consider configured tiers and compute default instance partitions for the segment
   *
   * @return InstancePartitions if the one can be derived from the given sorted tiers, null otherwise
   */
  @Nullable
  public static InstancePartitions getTieredInstancePartitionsForSegment(String tableNameWithType,
      String segmentName, @Nullable List<Tier> sortedTiers, HelixManager helixManager) {
    if (CollectionUtils.isEmpty(sortedTiers)) {
      return null;
    }

    // Find first applicable tier
    for (Tier tier : sortedTiers) {
      if (tier.getSegmentSelector().selectSegment(tableNameWithType, segmentName)) {
        // Compute default instance partitions
        PinotServerTierStorage storage = (PinotServerTierStorage) tier.getStorage();
        return InstancePartitionsUtils.computeDefaultInstancePartitionsForTag(helixManager, tableNameWithType,
            tier.getName(), storage.getServerTag());
      }
    }

    // Tier not found
    return null;
  }

  @Nullable
  public static String getDataDirForTier(TableConfig tableConfig, String tierName) {
    return getDataDirForTier(tableConfig, tierName, Collections.emptyMap());
  }

  @Nullable
  public static String getDataDirForTier(TableConfig tableConfig, String tierName,
      Map<String, Map<String, String>> instanceTierConfigs) {
    String tableNameWithType = tableConfig.getTableName();
    String dataDir = null;
    List<TierConfig> tierCfgs = tableConfig.getTierConfigsList();
    if (CollectionUtils.isNotEmpty(tierCfgs)) {
      TierConfig tierCfg = null;
      for (TierConfig tc : tierCfgs) {
        if (tierName.equals(tc.getName())) {
          tierCfg = tc;
          break;
        }
      }
      if (tierCfg != null) {
        Map<String, String> backendProps = tierCfg.getTierBackendProperties();
        if (backendProps == null) {
          LOGGER.debug("No backend props for tier: {} in TableConfig of table: {}", tierName, tableNameWithType);
        } else {
          dataDir = backendProps.get(CommonConstants.Tier.BACKEND_PROP_DATA_DIR);
          if (StringUtils.isNotEmpty(dataDir)) {
            LOGGER.debug("Got dataDir: {} for tier: {} in TableConfig of table: {}", dataDir, tierName,
                tableNameWithType);
            return dataDir;
          } else {
            LOGGER.debug("No dataDir for tier: {} in TableConfig of table: {}", tierName, tableNameWithType);
          }
        }
      }
    }
    // Check if there is data path defined in instance tier configs.
    Map<String, String> instanceCfgs = instanceTierConfigs.get(tierName);
    if (instanceCfgs != null) {
      // All instance config names are lower cased while being passed down here.
      dataDir = instanceCfgs.get(CommonConstants.Tier.BACKEND_PROP_DATA_DIR.toLowerCase());
    }
    LOGGER.debug("Got dataDir: {} for tier: {} for table: {} in instance configs", dataDir, tierName,
        tableNameWithType);
    return dataDir;
  }

  /**
   * Gets sorted list of tiers for given storage type from provided list of TierConfig
   */
  public static List<Tier> getSortedTiersForStorageType(List<TierConfig> tierConfigList, String storageType,
      HelixManager helixManager) {
    List<Tier> sortedTiers = new ArrayList<>();
    for (TierConfig tierConfig : tierConfigList) {
      if (storageType.equalsIgnoreCase(tierConfig.getStorageType())) {
        sortedTiers.add(TierFactory.getTier(tierConfig, helixManager));
      }
    }
    sortedTiers.sort(TierConfigUtils.getTierComparator());
    return sortedTiers;
  }

  /**
   * Gets sorted list of tiers from provided list of TierConfig
   */
  public static List<Tier> getSortedTiers(List<TierConfig> tierConfigList, HelixManager helixManager) {
    List<Tier> sortedTiers = new ArrayList<>();
    for (TierConfig tierConfig : tierConfigList) {
      sortedTiers.add(TierFactory.getTier(tierConfig, helixManager));
    }
    sortedTiers.sort(TierConfigUtils.getTierComparator());
    return sortedTiers;
  }

  /**
   * Comparator for sorting the {@link Tier}. In the sort order
   * 1) {@link FixedTierSegmentSelector} are always before others
   * 2) For {@link TimeBasedTierSegmentSelector}, tiers with an older age bucket appear before a younger age bucket,
   */
  public static Comparator<Tier> getTierComparator() {
    return (o1, o2) -> {
      TierSegmentSelector s1 = o1.getSegmentSelector();
      TierSegmentSelector s2 = o2.getSegmentSelector();
      if (TierFactory.FIXED_SEGMENT_SELECTOR_TYPE.equalsIgnoreCase(s1.getType())
          && TierFactory.FIXED_SEGMENT_SELECTOR_TYPE.equalsIgnoreCase(s2.getType())) {
        return 0;
      }
      if (TierFactory.FIXED_SEGMENT_SELECTOR_TYPE.equalsIgnoreCase(s1.getType())) {
        return -1;
      }
      if (TierFactory.FIXED_SEGMENT_SELECTOR_TYPE.equalsIgnoreCase(s2.getType())) {
        return 1;
      }
      Long period1 = ((TimeBasedTierSegmentSelector) s1).getSegmentAgeMillis();
      Long period2 = ((TimeBasedTierSegmentSelector) s2).getSegmentAgeMillis();
      return period2.compareTo(period1);
    };
  }
}
