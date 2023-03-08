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
package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseStats;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.core.transport.ServerRoutingInstance;


public class ExecutionStatsAggregator {
  private final List<QueryProcessingException> _processingExceptions = new ArrayList<>();
  private final Map<String, Map<String, String>> _operatorStats = new HashMap<>();
  private final Map<String, String> _traceInfo = new HashMap<>();
  private final Map<DataTable.MetadataKey, Object> _aggregatedStats = new HashMap<>();

  private final boolean _enableTrace;

  public ExecutionStatsAggregator(boolean enableTrace) {
    _enableTrace = enableTrace;
  }

  public void aggregate(ServerRoutingInstance routingInstance, DataTable dataTable) {
    aggregate(routingInstance, dataTable.getMetadata(), dataTable.getExceptions());
  }

  public synchronized void aggregate(@Nullable ServerRoutingInstance routingInstance, Map<String, String> metadata,
      Map<Integer, String> exceptions) {

    // Reduce on trace info.
    if (_enableTrace && routingInstance != null) {
      _traceInfo.put(routingInstance.getShortName(),
          metadata.getOrDefault(DataTable.MetadataKey.TRACE_INFO.getName(), ""));
    }

    String operatorId = metadata.get(DataTable.MetadataKey.OPERATOR_ID.getName());
    if (operatorId != null) {
      if (_enableTrace) {
        _operatorStats.put(operatorId, metadata);
      } else {
        _operatorStats.put(operatorId, new HashMap<>());
      }
    }


    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      DataTable.MetadataKey metadataKey = DataTable.MetadataKey.getByName(key);

      if (metadataKey == null) {
        continue;
      }

      if (metadataKey == DataTable.MetadataKey.TRACE_INFO || metadataKey == DataTable.MetadataKey.OPERATOR_ID) {
        continue;
      }

      switch (metadataKey.getValueType()) {
        case INT:
        case LONG: {
          _aggregatedStats.put(metadataKey,
              (Long) _aggregatedStats.getOrDefault(metadataKey, 0L) + Long.parseLong(value));
          break;
        }
        case STRING: {
          try {
            if (!isBooleanString(value)) {
              _aggregatedStats.merge(metadataKey, value,
                  (a, b) -> a + DataTable.MetadataKey.MULTI_VALUE_STRING_SEPARATOR + b);
            } else {
              boolean boolVal = Boolean.parseBoolean(value);
              boolean existingBoolVal =
                  Boolean.parseBoolean((String) _aggregatedStats.getOrDefault(metadataKey, "false"));
              _aggregatedStats.put(metadataKey, String.valueOf(existingBoolVal | boolVal));
            }
          } catch (Exception e) {
            throw new RuntimeException(String.format("Unsupported data type for key: %s value: %s", key, value), e);
          }
          break;
        }
        default: {
          throw new RuntimeException("Unsupported value type: " + metadataKey.getValueType());
        }
      }
    }

    // Reduce on exceptions.
    for (int key : exceptions.keySet()) {
      _processingExceptions.add(new QueryProcessingException(key, exceptions.get(key)));
    }
  }

  private boolean isBooleanString(String value) {
    return Objects.equals(value, "true") || Objects.equals(value, "false");
  }

  public void setStats(BrokerResponseNative brokerResponseNative) {
    setStats(null, brokerResponseNative, null);
  }

  public void setStats(@Nullable String rawTableName, BrokerResponseNative brokerResponseNative,
      @Nullable BrokerMetrics brokerMetrics) {
    // set exception
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    processingExceptions.addAll(_processingExceptions);

    // add all trace.
    if (_enableTrace) {
      brokerResponseNative.getTraceInfo().putAll(_traceInfo);
    }

    brokerResponseNative.setAggregatedStats(_aggregatedStats);

    // Update broker metrics.
    if (brokerMetrics != null && rawTableName != null) {
      addBrokerMetrics(rawTableName, brokerMetrics, brokerResponseNative);
    }
  }

  private void addBrokerMetrics(String rawTableName, BrokerMetrics brokerMetrics,
      BrokerResponseNative brokerResponseNative) {
    brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED,
        brokerResponseNative.getNumDocsScanned());
    brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER,
        brokerResponseNative.getNumEntriesScannedInFilter());
    brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_POST_FILTER,
        brokerResponseNative.getNumEntriesScannedPostFilter());
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_THREAD_CPU_TIME_NS,
        brokerResponseNative.getOfflineThreadCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_THREAD_CPU_TIME_NS,
        brokerResponseNative.getRealtimeThreadCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_SYSTEM_ACTIVITIES_CPU_TIME_NS,
        brokerResponseNative.getOfflineSystemActivitiesCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_SYSTEM_ACTIVITIES_CPU_TIME_NS,
        brokerResponseNative.getRealtimeSystemActivitiesCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_RESPONSE_SER_CPU_TIME_NS,
        brokerResponseNative.getOfflineResponseSerializationCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_RESPONSE_SER_CPU_TIME_NS,
        brokerResponseNative.getRealtimeResponseSerializationCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.OFFLINE_TOTAL_CPU_TIME_NS,
        brokerResponseNative.getOfflineTotalCpuTimeNs(), TimeUnit.NANOSECONDS);
    brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.REALTIME_TOTAL_CPU_TIME_NS,
        brokerResponseNative.getRealtimeTotalCpuTimeNs(), TimeUnit.NANOSECONDS);

    if (_aggregatedStats.containsKey(DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS)) {
      brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.FRESHNESS_LAG_MS,
          System.currentTimeMillis() - brokerResponseNative.getMinConsumingFreshnessTimeMs(), TimeUnit.MILLISECONDS);
    }
  }

  public void setStageLevelStats(@Nullable String rawTableName, BrokerResponseStats brokerResponseStats,
      @Nullable BrokerMetrics brokerMetrics) {
    setStats(rawTableName, brokerResponseStats, brokerMetrics);
    brokerResponseStats.setOperatorStats(_operatorStats);
  }

  private long getLongValue(DataTable.MetadataKey metadataKey) {
    return (Long) _aggregatedStats.getOrDefault(metadataKey, 0L);
  }
}
