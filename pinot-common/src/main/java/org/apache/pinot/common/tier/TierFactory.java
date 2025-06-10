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
package org.apache.pinot.common.tier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.config.table.TierConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to create and sort {@link Tier}
 */
public final class TierFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TierFactory.class);
  public static final String TIME_SEGMENT_SELECTOR_TYPE = "time";
  public static final String FIXED_SEGMENT_SELECTOR_TYPE = "fixed";
  public static final String PINOT_SERVER_STORAGE_TYPE = "pinot_server";

  private TierFactory() {
  }

  /**
   * Constructs a {@link Tier} from the {@link TierConfig} in the table config
   */
  public static Tier getTier(TierConfig tierConfig) {
    return getTier(tierConfig, null);
  }

  public static Tier getTier(TierConfig tierConfig, @Nullable Set<String> providedSegmentsForTier) {
    TierSegmentSelector segmentSelector;
    TierStorage storageSelector;
    String segmentSelectorType = tierConfig.getSegmentSelectorType();
    if (providedSegmentsForTier != null) {
      LOGGER.debug("Provided segments: {} for tier: {}", providedSegmentsForTier, tierConfig.getName());
      segmentSelector = new FixedTierSegmentSelector(providedSegmentsForTier);
    } else if (segmentSelectorType.equalsIgnoreCase(TierFactory.TIME_SEGMENT_SELECTOR_TYPE)) {
      segmentSelector = new TimeBasedTierSegmentSelector(tierConfig.getSegmentAge());
    } else if (segmentSelectorType.equalsIgnoreCase(TierFactory.FIXED_SEGMENT_SELECTOR_TYPE)) {
      List<String> segments = tierConfig.getSegmentList();
      segmentSelector =
          new FixedTierSegmentSelector(CollectionUtils.isEmpty(segments) ? Set.of() : new HashSet<>(segments));
    } else {
      throw new IllegalStateException("Unsupported segmentSelectorType: " + segmentSelectorType);
    }

    String storageSelectorType = tierConfig.getStorageType();
    if (storageSelectorType.equalsIgnoreCase(TierFactory.PINOT_SERVER_STORAGE_TYPE)) {
      storageSelector = new PinotServerTierStorage(tierConfig.getServerTag(), tierConfig.getTierBackend(),
          tierConfig.getTierBackendProperties());
    } else {
      throw new IllegalStateException("Unsupported storageType: " + storageSelectorType);
    }

    return new Tier(tierConfig.getName(), segmentSelector, storageSelector);
  }
}
