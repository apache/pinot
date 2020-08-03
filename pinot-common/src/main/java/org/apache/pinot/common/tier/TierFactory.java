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

import com.google.common.base.Preconditions;
import java.util.Comparator;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.config.table.TierConfig;


/**
 * Factory class to create and sort {@link Tier}
 */
public final class TierFactory {

  public static final String TIME_BASED_SEGMENT_SELECTOR_TYPE = "timeBased";
  public static final String PINOT_SERVER_STORAGE_TYPE = "pinotServer";

  private TierFactory() {
  }

  /**
   * Constructs a {@link Tier} from the {@link TierConfig} in the table config
   */
  public static Tier getTier(TierConfig tierConfig, HelixManager helixManager) {
    TierSegmentSelector segmentSelector;
    TierStorage storageSelector;

    String segmentSelectorType = tierConfig.getSegmentSelectorType();
    if (segmentSelectorType.equalsIgnoreCase(TIME_BASED_SEGMENT_SELECTOR_TYPE)) {
      segmentSelector = new TimeBasedTierSegmentSelector(helixManager, tierConfig.getSegmentAge());
    } else {
      throw new IllegalStateException("Unsupported segmentSelectorType: " + segmentSelectorType);
    }

    String storageSelectorType = tierConfig.getStorageType();
    if (storageSelectorType.equalsIgnoreCase(PINOT_SERVER_STORAGE_TYPE)) {
      storageSelector = new PinotServerTierStorage(tierConfig.getServerTag());
    } else {
      throw new IllegalStateException("Unsupported storageType: " + storageSelectorType);
    }

    return new Tier(tierConfig.getName(), segmentSelector, storageSelector);
  }

  /**
   * Comparator for sorting the {@link Tier}.
   * As of now, we have only 1 type of {@link TierSegmentSelector} and 1 type of {@link TierStorage}.
   * Tier with an older age bucket in {@link TimeBasedTierSegmentSelector} should appear before a younger age bucket, in sort order
   * TODO: As we add more types, this logic needs to be upgraded
   */
  public static Comparator<Tier> getTierComparator() {
    return (o1, o2) -> {
      TierSegmentSelector s1 = o1.getSegmentSelector();
      TierSegmentSelector s2 = o2.getSegmentSelector();
      Preconditions
          .checkState(TIME_BASED_SEGMENT_SELECTOR_TYPE.equals(s1.getType()), "Unsupported segmentSelectorType class %s",
              s1.getClass());
      Preconditions
          .checkState(TIME_BASED_SEGMENT_SELECTOR_TYPE.equals(s2.getType()), "Unsupported segmentSelectorType class %s",
              s2.getClass());
      Long period1 = ((TimeBasedTierSegmentSelector) s1).getSegmentAgeMillis();
      Long period2 = ((TimeBasedTierSegmentSelector) s2).getSegmentAgeMillis();
      return period2.compareTo(period1);
    };
  }
}
