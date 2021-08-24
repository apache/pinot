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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.tier.TierStorage;
import org.apache.pinot.common.tier.TimeBasedTierSegmentSelector;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TierConfig;


/**
 * Util methods for TierConfig
 */
public final class TierConfigUtils {

  private TierConfigUtils() {
  }

  /**
   * Returns whether relocation of segments to tiers has been enabled for this table
   */
  public static boolean shouldRelocateToTiers(TableConfig tableConfig) {
    return CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList());
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
   * Comparator for sorting the {@link Tier}.
   * As of now, we have only 1 type of {@link TierSegmentSelector} and 1 type of {@link TierStorage}.
   * Tier with an older age bucket in {@link TimeBasedTierSegmentSelector} should appear before a younger age bucket,
   * in sort order
   * TODO: As we add more types, this logic needs to be upgraded
   */
  public static Comparator<Tier> getTierComparator() {
    return (o1, o2) -> {
      TierSegmentSelector s1 = o1.getSegmentSelector();
      TierSegmentSelector s2 = o2.getSegmentSelector();
      Preconditions.checkState(TierFactory.TIME_SEGMENT_SELECTOR_TYPE.equalsIgnoreCase(s1.getType()),
          "Unsupported segmentSelectorType class %s", s1.getClass());
      Preconditions.checkState(TierFactory.TIME_SEGMENT_SELECTOR_TYPE.equalsIgnoreCase(s2.getType()),
          "Unsupported segmentSelectorType class %s", s2.getClass());
      Long period1 = ((TimeBasedTierSegmentSelector) s1).getSegmentAgeMillis();
      Long period2 = ((TimeBasedTierSegmentSelector) s2).getSegmentAgeMillis();
      return period2.compareTo(period1);
    };
  }
}
