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


public class IngestionSourceErrorRecorder {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionSourceErrorRecorder.class);
  private static final long LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  @VisibleForTesting
  static final AtomicLong LAST_RECORDING_FAILURE_LOG_MS = new AtomicLong();

  private final ServerMetrics _serverMetrics;
  private final String _tableNameWithType;
  private final Set<Integer> _partitionsWritten = new HashSet<>();
  private final Map<IngestionSourceErrorCategory, Long> _lastLoggedMs =
      new EnumMap<>(IngestionSourceErrorCategory.class);
  private boolean _closed;

  public IngestionSourceErrorRecorder(ServerMetrics serverMetrics, String tableNameWithType) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
  }

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
