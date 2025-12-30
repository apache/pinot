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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.config.table.TierConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for tier segment selector factories.
 * Plugins can register custom selector factories using the {@link TierSegmentSelectorFactory} interface.
 */
public final class TierSegmentSelectorRegistry {
  private TierSegmentSelectorRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TierSegmentSelectorRegistry.class);

  /**
   * Functional interface for creating TierSegmentSelector instances.
   */
  @FunctionalInterface
  public interface TierSegmentSelectorFactory {
    /**
     * Creates a TierSegmentSelector from the given TierConfig.
     */
    TierSegmentSelector create(TierConfig tierConfig);
  }

  private static final ConcurrentHashMap<String, TierSegmentSelectorFactory> FACTORIES = new ConcurrentHashMap<>();

  static {
    // Register default selectors
    register(TierFactory.TIME_SEGMENT_SELECTOR_TYPE, cfg -> new TimeBasedTierSegmentSelector(cfg.getSegmentAge()));
    register(TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, cfg -> {
      List<String> segments = cfg.getSegmentList();
      Set<String> segmentSet = CollectionUtils.isEmpty(segments) ? new HashSet<>() : new HashSet<>(segments);
      return new FixedTierSegmentSelector(segmentSet);
    });
  }

  /**
   * Registers a factory for the provided selector type.
   *
   * @param selectorType Type string used in {@link TierConfig#getSegmentSelectorType()}.
   * @param factory Factory that creates TierSegmentSelector instances.
   */
  public static void register(String selectorType, TierSegmentSelectorFactory factory) {
    String key = selectorType.toLowerCase();
    TierSegmentSelectorFactory previous = FACTORIES.put(key, factory);
    if (previous == null) {
      LOGGER.info("Registered tier segment selector factory for type '{}'", selectorType);
    } else {
      LOGGER.warn("Replacing existing selector factory for type '{}'", selectorType);
    }
  }

  /**
   * Creates a selector for the provided tier config.
   */
  public static TierSegmentSelector create(TierConfig tierConfig) {
    String selectorType = tierConfig.getSegmentSelectorType();
    String key = selectorType.toLowerCase();
    TierSegmentSelectorFactory factory = FACTORIES.get(key);
    if (factory == null) {
      throw new IllegalStateException("Unsupported segmentSelectorType: " + selectorType);
    }
    return factory.create(tierConfig);
  }
}
