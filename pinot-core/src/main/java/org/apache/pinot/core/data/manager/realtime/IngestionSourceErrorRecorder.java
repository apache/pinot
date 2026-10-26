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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Records the stream source failures a realtime table's consumers and its ingestion delay tracker see: a timestamp
/// gauge per partition and category, and a WARN line per write, rate-limited per partition.
///
/// [IngestionSourceErrorCategory#AUTH] is written at every refusal, whether or not Pinot retries.
/// [IngestionSourceErrorCategory#SOURCE_READ] is written only once Pinot gives up on the source, so a transient
/// failure it recovers from writes nothing. The gauges record when a failure last happened: they are never cleared on
/// success or when a partition stops consuming, only when the table data manager shuts down.
///
/// Recording never throws, so it never changes consumption. The WARN line carries no exception message, which can
/// hold principals, ARNs or account ids.
public class IngestionSourceErrorRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionSourceErrorRecorder.class);
  private static final long LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final LongSupplier _clock;
  private final Set<Integer> _partitionsWritten = ConcurrentHashMap.newKeySet();
  // Keyed by partition and category
  private final Map<String, Long> _lastLoggedMs = new ConcurrentHashMap<>();

  public IngestionSourceErrorRecorder(ServerMetrics serverMetrics, String tableNameWithType) {
    this(serverMetrics, tableNameWithType, System::currentTimeMillis);
  }

  @VisibleForTesting
  IngestionSourceErrorRecorder(ServerMetrics serverMetrics, String tableNameWithType, LongSupplier clock) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _clock = clock;
  }

  /// Classifies a failure from the stream source of `topic` and records it for `partitionIds` if it is the source's
  /// verdict and due to be written.
  ///
  /// @param site where Pinot saw the failure, for the log line
  /// @param givingUp whether Pinot gives up on the source with this failure, rather than retrying it
  /// @return the category written, or null when nothing was written
  @Nullable
  public IngestionSourceErrorCategory record(String site, Throwable failure, boolean givingUp, String topic,
      Collection<Integer> partitionIds) {
    try {
      IngestionSourceErrorClassification classification =
          IngestionSourceErrorClassifier.classify(failure).orElse(null);
      if (classification == null || partitionIds.isEmpty()) {
        return null;
      }
      IngestionSourceErrorCategory category = classification.getCategory();
      if (category == IngestionSourceErrorCategory.SOURCE_READ && !givingUp) {
        return null;
      }
      ServerGauge gauge = category == IngestionSourceErrorCategory.AUTH
          ? ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS
          : ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS;
      long nowMs = _clock.getAsLong();
      boolean logDue = false;
      for (int partitionId : partitionIds) {
        _partitionsWritten.add(partitionId);
        _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, partitionId, gauge,
            TimeUnit.MILLISECONDS.toSeconds(nowMs));
        Long lastLoggedMs = _lastLoggedMs.get(partitionId + "/" + category);
        if (lastLoggedMs == null || nowMs - lastLoggedMs >= LOG_INTERVAL_MS) {
          logDue = true;
        }
      }
      if (logDue) {
        for (int partitionId : partitionIds) {
          _lastLoggedMs.put(partitionId + "/" + category, nowMs);
        }
        LOGGER.warn("Ingestion source {} failure for table: {}, topic: {}, partitions: {}, at: {}: {}", category,
            _tableNameWithType, topic, partitionIds, site, classification);
      }
      return category;
    } catch (RuntimeException e) {
      LOGGER.debug("Failed to record an ingestion source failure for table: {}", _tableNameWithType, e);
      return null;
    }
  }

  /// Removes the gauges of every partition written so far.
  public void removeMetrics() {
    for (int partitionId : _partitionsWritten) {
      _serverMetrics.removePartitionGauge(_tableNameWithType, partitionId,
          ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS);
      _serverMetrics.removePartitionGauge(_tableNameWithType, partitionId,
          ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS);
    }
    _partitionsWritten.clear();
    _lastLoggedMs.clear();
  }
}
