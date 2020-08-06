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
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TierConfig;


/**
 * Util methods for TierConfig
 */
public class TierConfigUtils {

  /**
   * Returns whether relocation of segments to tiers has been enabled for this table
   */
  public static boolean shouldRelocateToTiers(TableConfig tableConfig) {
    return CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList());
  }

  /**
   * Gets tiers for given storage type from provided list of TierConfig
   */
  public static List<Tier> getTiersForStorageType(List<TierConfig> tierConfigList, String storageType,
      HelixManager helixManager) {
    List<Tier> tiers = new ArrayList<>();
    for (TierConfig tierConfig : tierConfigList) {
      if (storageType.equals(tierConfig.getStorageType())) {
        tiers.add(TierFactory.getTier(tierConfig, helixManager));
      }
    }
    return tiers;
  }
}
