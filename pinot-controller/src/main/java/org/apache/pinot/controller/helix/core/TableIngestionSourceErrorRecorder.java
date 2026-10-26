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
package org.apache.pinot.controller.helix.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Records the stream source failures the controller sees while fetching a realtime table's stream metadata: a
/// timestamp gauge per table and category, and a WARN line per write, rate-limited per table.
///
/// Every controller site records only once it has given up on the source (the fetch exhausted its retries, a topic
/// was skipped as permanently failed, or the plugin reported a failure it swallows), so both categories are written.
/// The gauges are never cleared on success; [org.apache.pinot.controller.helix.SegmentStatusChecker] removes them
/// with the table's other gauges. Recording never throws.
public final class TableIngestionSourceErrorRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableIngestionSourceErrorRecorder.class);
  // Keyed by table and category
  private static final Cache<String, Boolean> RECENTLY_LOGGED =
      CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();

  private TableIngestionSourceErrorRecorder() {
  }

  /// Classifies a failure of the table's stream source and records it if it is the source's verdict.
  ///
  /// @param site where the controller saw the failure, for the log line
  /// @return the category written, or null when nothing was written
  @Nullable
  public static IngestionSourceErrorCategory record(String tableNameWithType, String site,
      @Nullable Throwable failure) {
    if (failure == null) {
      return null;
    }
    try {
      IngestionSourceErrorClassification classification =
          IngestionSourceErrorClassifier.classify(failure).orElse(null);
      if (classification == null) {
        return null;
      }
      IngestionSourceErrorCategory category = classification.getCategory();
      ControllerGauge gauge = category == IngestionSourceErrorCategory.AUTH
          ? ControllerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS
          : ControllerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS;
      ControllerMetrics.get().setValueOfTableGauge(tableNameWithType, gauge,
          TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
      if (RECENTLY_LOGGED.asMap().putIfAbsent(tableNameWithType + "/" + category, Boolean.TRUE) == null) {
        LOGGER.warn("Ingestion source {} failure for table: {}, at: {}: {}", category, tableNameWithType, site,
            classification);
      }
      return category;
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to record an ingestion source failure for table: {}", tableNameWithType, e);
      return null;
    }
  }
}
