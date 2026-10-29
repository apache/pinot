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

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorCategory;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassification;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Records the stream source failures a realtime table's consumers and its ingestion delay tracker see: a timestamp
/// gauge per partition and category, and a WARN line per write, rate-limited per category.
///
/// [IngestionSourceErrorCategory#AUTH] is written at every refusal, whether or not Pinot retries.
/// [IngestionSourceErrorCategory#SOURCE_READ] is written only once Pinot gives up on the source, so a transient
/// failure it recovers from writes nothing. The gauges record when a failure last happened: they are never cleared on
/// success or when a partition stops consuming, only when the table data manager shuts down ([#close]).
///
/// Recording never throws, so it never changes consumption. The WARN line carries no exception message, which can
/// hold principals, ARNs or account ids.
public class IngestionSourceErrorRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionSourceErrorRecorder.class);
  private static final long LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  // A failure in recording itself, such as a metrics registry that throws: logged from any consumer thread, at most
  // once per interval for the process
  private static final AtomicLong LAST_RECORDING_FAILURE_LOG_MS = new AtomicLong();

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  // Guarded by this: gauge writes and close() are serialized, so once close() returns nothing writes or removes a gauge
  // of this table again, even for a table recreated under the same name
  private final Set<Integer> _partitionsWritten = new HashSet<>();
  private final Map<IngestionSourceErrorCategory, Long> _lastLoggedMs =
      new EnumMap<>(IngestionSourceErrorCategory.class);
  private boolean _closed;

  public IngestionSourceErrorRecorder(ServerMetrics serverMetrics, String tableNameWithType) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
  }

  /// Classifies a failure from the stream source of `topic` and records it for `partitionIds` if it is the source's
  /// verdict and due to be written.
  ///
  /// @param site where Pinot saw the failure, for the log line
  /// @param givingUp whether Pinot gives up on the source with this failure, rather than retrying it
  public void record(String site, Throwable failure, boolean givingUp, String topic,
      Collection<Integer> partitionIds) {
    if (partitionIds.isEmpty()) {
      return;
    }
    try {
      IngestionSourceErrorClassification classification =
          IngestionSourceErrorClassifier.classify(failure).orElse(null);
      if (classification == null) {
        return;
      }
      IngestionSourceErrorCategory category = classification.getCategory();
      if (category == IngestionSourceErrorCategory.SOURCE_READ && !givingUp) {
        return;
      }
      ServerGauge gauge = gaugeOf(category);
      long nowMs = System.currentTimeMillis();
      boolean logDue;
      synchronized (this) {
        // A consumer thread can outlive the table's shutdown
        if (_closed) {
          return;
        }
        for (int partitionId : partitionIds) {
          _partitionsWritten.add(partitionId);
          _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, partitionId, gauge,
              TimeUnit.MILLISECONDS.toSeconds(nowMs));
        }
        Long lastLoggedMs = _lastLoggedMs.get(category);
        logDue = lastLoggedMs == null || nowMs - lastLoggedMs >= LOG_INTERVAL_MS;
        if (logDue) {
          _lastLoggedMs.put(category, nowMs);
        }
      }
      if (logDue) {
        LOGGER.warn("Ingestion source {} failure for table: {}, topic: {}, partitions: {}, at: {}: {}", category,
            _tableNameWithType, topic, partitionIds, site, classification);
      }
    } catch (RuntimeException e) {
      long nowMs = System.currentTimeMillis();
      long lastLoggedMs = LAST_RECORDING_FAILURE_LOG_MS.get();
      if (nowMs - lastLoggedMs >= LOG_INTERVAL_MS && LAST_RECORDING_FAILURE_LOG_MS.compareAndSet(lastLoggedMs, nowMs)) {
        LOGGER.warn("Failed to record an ingestion source failure for table: {}", _tableNameWithType, e);
      }
    }
  }

  /// Stops recording and removes the gauges of every partition written so far. Waits for a write in progress.
  public synchronized void close() {
    _closed = true;
    for (int partitionId : _partitionsWritten) {
      for (IngestionSourceErrorCategory category : IngestionSourceErrorCategory.values()) {
        _serverMetrics.removePartitionGauge(_tableNameWithType, partitionId, gaugeOf(category));
      }
    }
  }

  private static ServerGauge gaugeOf(IngestionSourceErrorCategory category) {
    return category == IngestionSourceErrorCategory.AUTH ? ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS
        : ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS;
  }
}
