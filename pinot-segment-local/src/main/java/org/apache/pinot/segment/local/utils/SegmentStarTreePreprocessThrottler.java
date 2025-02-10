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
package org.apache.pinot.segment.local.utils;

import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to throttle the total concurrent startree index rebuilds on a given Pinot server.
 */
public class SegmentStarTreePreprocessThrottler extends BaseSegmentPreprocessThrottler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStarTreePreprocessThrottler.class);

  /**
   * @param maxStarTreePreprocessConcurrency configured StarTree index preprocessing concurrency
   * @param maxStarTreePreprocessConcurrencyBeforeServingQueries configured preprocessing concurrency before serving
   *                                                             queries
   * @param isServingQueries whether the server is ready to serve queries or not
   */
  public SegmentStarTreePreprocessThrottler(int maxStarTreePreprocessConcurrency,
      int maxStarTreePreprocessConcurrencyBeforeServingQueries, boolean isServingQueries) {
    super(maxStarTreePreprocessConcurrency, maxStarTreePreprocessConcurrencyBeforeServingQueries, isServingQueries,
        LOGGER);
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentStarTreePreprocessThrottler configs with unchanged clusterConfigs");
      return;
    }

    LOGGER.info("Updating SegmentStarTreePreprocessThrottler configs with latest clusterConfigs");
    handleMaxPreprocessConcurrencyChange(changedConfigs, clusterConfigs,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
    handleMaxPreprocessConcurrencyBeforeServingQueriesChange(changedConfigs, clusterConfigs,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    LOGGER.info("Updated SegmentStarTreePreprocessThrottler configs with latest clusterConfigs");
  }
}
