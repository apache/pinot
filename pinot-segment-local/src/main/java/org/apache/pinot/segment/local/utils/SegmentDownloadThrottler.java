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
 * Used to throttle the total concurrent segment downloads that can happen on a given Pinot server. This throttler
 * works with the table level semaphore for deep store downloads. This throttler will also be used for peer downloads.
 * Note: the table level semaphore is only used for downloads from deep store.
 * - The table level semaphore is to ensure that each table's segment download is not starved under scenarios where a
 *   table is flooded with a large number of new / updated segments.
 * - This server level semaphore is meant to protect the Pinot server from resource exhaustion due to too many parallel
 *   downloads occurring at once.
 * Lock ordering (for download from deep store):
 * - Table semaphore
 * - Server semaphore
 */
public class SegmentDownloadThrottler extends BaseSegmentOperationsThrottler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDownloadThrottler.class);

  /**
   * @param maxDownloadConcurrency configured preprocessing concurrency
   * @param maxDownloadConcurrencyBeforeServingQueries configured preprocessing concurrency before serving queries
   * @param isServingQueries whether the server is ready to serve queries or not
   */
  public SegmentDownloadThrottler(int maxDownloadConcurrency, int maxDownloadConcurrencyBeforeServingQueries,
      boolean isServingQueries) {
    super(maxDownloadConcurrency, maxDownloadConcurrencyBeforeServingQueries, isServingQueries, LOGGER);
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (CollectionUtils.isEmpty(changedConfigs)) {
      LOGGER.info("Skip updating SegmentDownloadThrottler configs with unchanged clusterConfigs");
      return;
    }

    LOGGER.info("Updating SegmentDownloadThrottler configs with latest clusterConfigs");
    handleMaxConcurrencyChange(changedConfigs, clusterConfigs,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM);
    handleMaxConcurrencyBeforeServingQueriesChange(changedConfigs, clusterConfigs,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES,
        CommonConstants.Helix.DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES);
    LOGGER.info("Updated SegmentDownloadThrottler configs with latest clusterConfigs");
  }
}
