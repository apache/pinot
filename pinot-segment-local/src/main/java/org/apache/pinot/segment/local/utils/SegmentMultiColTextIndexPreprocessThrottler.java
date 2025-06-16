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
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Used to throttle the total concurrent multi-col text index rebuilds on a given Pinot server.
 */
public class SegmentMultiColTextIndexPreprocessThrottler extends BaseSegmentOperationsThrottler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMultiColTextIndexPreprocessThrottler.class);

  /**
   * @param maxMultiColTextIndexPreprocessConcurrency configured multi-Col text index preprocessing concurrency
   * @param maxMultiColTextIndexPreprocessConcurrencyBeforeServingQueries configured preprocessing concurrency before
   *                                                                     serving
   *                                                             queries
   * @param isServingQueries whether the server is ready to serve queries or not
   */
  public SegmentMultiColTextIndexPreprocessThrottler(int maxMultiColTextIndexPreprocessConcurrency,
      int maxMultiColTextIndexPreprocessConcurrencyBeforeServingQueries, boolean isServingQueries) {
    super(maxMultiColTextIndexPreprocessConcurrency, maxMultiColTextIndexPreprocessConcurrencyBeforeServingQueries,
        isServingQueries, LOGGER);
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentMultiColTextIndexPreprocessThrottler configs with unchanged clusterConfigs");
      return;
    }

    LOGGER.info("Updating SegmentMultiColTextIndexPreprocessThrottler configs with latest clusterConfigs");
    handleMaxConcurrencyChange(changedConfigs, clusterConfigs,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM);
    handleMaxConcurrencyBeforeServingQueriesChange(changedConfigs, clusterConfigs,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES);
    LOGGER.info("Updated SegmentMultiColTextIndexPreprocessThrottler configs with latest clusterConfigs");
  }

  @Override
  public void updateThresholdMetric(int value) {
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.SEGMENT_MULTI_COL_TEXT_INDEX_PREPROCESS_THROTTLE_THRESHOLD, value);
  }

  @Override
  public void updateCountMetric(int value) {
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.SEGMENT_MULTI_COL_TEXT_INDEX_PREPROCESS_COUNT, value);
  }
}
