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
package org.apache.pinot.broker.querylog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.RequestContext.FanoutType;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Immutable representation of a single broker query log row that is exposed through the system table.
 */
public class QueryLogRecord {
  private final long _logTimestampMs;
  private final long _requestId;
  private final String _tableName;
  private final String _brokerId;
  private final String _clientIp;
  private final String _query;
  private final String _queryEngine;
  private final long _requestArrivalTimeMs;
  private final long _timeMs;
  private final long _brokerReduceTimeMs;
  private final long _numDocsScanned;
  private final long _totalDocs;
  private final long _numEntriesScannedInFilter;
  private final long _numEntriesScannedPostFilter;
  private final long _numSegmentsQueried;
  private final long _numSegmentsProcessed;
  private final long _numSegmentsMatched;
  private final long _numConsumingSegmentsQueried;
  private final long _numConsumingSegmentsProcessed;
  private final long _numConsumingSegmentsMatched;
  private final long _numUnavailableSegments;
  private final long _minConsumingFreshnessTimeMs;
  private final int _numServersResponded;
  private final int _numServersQueried;
  private final boolean _groupsTrimmed;
  private final boolean _groupLimitReached;
  private final boolean _groupWarningLimitReached;
  private final long _numExceptions;
  private final String _exceptions;
  private final String _serverStats;
  private final long _offlineTotalCpuTimeNs;
  private final long _offlineThreadCpuTimeNs;
  private final long _offlineSystemActivitiesCpuTimeNs;
  private final long _offlineResponseSerializationCpuTimeNs;
  private final long _realtimeTotalCpuTimeNs;
  private final long _realtimeThreadCpuTimeNs;
  private final long _realtimeSystemActivitiesCpuTimeNs;
  private final long _realtimeResponseSerializationCpuTimeNs;
  private final long _offlineTotalMemAllocatedBytes;
  private final long _offlineThreadMemAllocatedBytes;
  private final long _offlineResponseSerMemAllocatedBytes;
  private final long _realtimeTotalMemAllocatedBytes;
  private final long _realtimeThreadMemAllocatedBytes;
  private final long _realtimeResponseSerMemAllocatedBytes;
  private final int[] _pools;
  private final boolean _partialResult;
  private final boolean _rlsFiltersApplied;
  private final int _numRowsResultSet;
  private final String[] _tablesQueried;
  private final String _traceInfoJson;
  private final String _fanoutType;
  private final String _offlineServerTenant;
  private final String _realtimeServerTenant;

  QueryLogRecord(long logTimestampMs, long requestId, String tableName, String brokerId, String clientIp,
      String query, String queryEngine, long requestArrivalTimeMs, long timeMs, long brokerReduceTimeMs,
      long numDocsScanned, long totalDocs, long numEntriesScannedInFilter, long numEntriesScannedPostFilter,
      long numSegmentsQueried, long numSegmentsProcessed, long numSegmentsMatched, long numConsumingSegmentsQueried,
      long numConsumingSegmentsProcessed, long numConsumingSegmentsMatched, long numUnavailableSegments,
      long minConsumingFreshnessTimeMs, int numServersResponded, int numServersQueried, boolean groupsTrimmed,
      boolean groupLimitReached, boolean groupWarningLimitReached, long numExceptions, String exceptions,
      String serverStats, long offlineTotalCpuTimeNs, long offlineThreadCpuTimeNs,
      long offlineSystemActivitiesCpuTimeNs, long offlineResponseSerializationCpuTimeNs, long realtimeTotalCpuTimeNs,
      long realtimeThreadCpuTimeNs, long realtimeSystemActivitiesCpuTimeNs,
      long realtimeResponseSerializationCpuTimeNs, long offlineTotalMemAllocatedBytes,
      long offlineThreadMemAllocatedBytes, long offlineResponseSerMemAllocatedBytes,
      long realtimeTotalMemAllocatedBytes, long realtimeThreadMemAllocatedBytes,
      long realtimeResponseSerMemAllocatedBytes, int[] pools, boolean partialResult, boolean rlsFiltersApplied,
      int numRowsResultSet, String[] tablesQueried, String traceInfoJson, String fanoutType,
      String offlineServerTenant, String realtimeServerTenant) {
    _logTimestampMs = logTimestampMs;
    _requestId = requestId;
    _tableName = tableName;
    _brokerId = brokerId;
    _clientIp = clientIp;
    _query = query;
    _queryEngine = queryEngine;
    _requestArrivalTimeMs = requestArrivalTimeMs;
    _timeMs = timeMs;
    _brokerReduceTimeMs = brokerReduceTimeMs;
    _numDocsScanned = numDocsScanned;
    _totalDocs = totalDocs;
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    _numSegmentsQueried = numSegmentsQueried;
    _numSegmentsProcessed = numSegmentsProcessed;
    _numSegmentsMatched = numSegmentsMatched;
    _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
    _numUnavailableSegments = numUnavailableSegments;
    _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
    _numServersResponded = numServersResponded;
    _numServersQueried = numServersQueried;
    _groupsTrimmed = groupsTrimmed;
    _groupLimitReached = groupLimitReached;
    _groupWarningLimitReached = groupWarningLimitReached;
    _numExceptions = numExceptions;
    _exceptions = exceptions;
    _serverStats = serverStats;
    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
    _offlineThreadCpuTimeNs = offlineThreadCpuTimeNs;
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
    _realtimeThreadCpuTimeNs = realtimeThreadCpuTimeNs;
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
    _offlineTotalMemAllocatedBytes = offlineTotalMemAllocatedBytes;
    _offlineThreadMemAllocatedBytes = offlineThreadMemAllocatedBytes;
    _offlineResponseSerMemAllocatedBytes = offlineResponseSerMemAllocatedBytes;
    _realtimeTotalMemAllocatedBytes = realtimeTotalMemAllocatedBytes;
    _realtimeThreadMemAllocatedBytes = realtimeThreadMemAllocatedBytes;
    _realtimeResponseSerMemAllocatedBytes = realtimeResponseSerMemAllocatedBytes;
    _pools = pools != null ? pools : new int[0];
    _partialResult = partialResult;
    _rlsFiltersApplied = rlsFiltersApplied;
    _numRowsResultSet = numRowsResultSet;
    _tablesQueried = tablesQueried != null ? tablesQueried : new String[0];
    _traceInfoJson = traceInfoJson != null ? traceInfoJson : "{}";
    _fanoutType = fanoutType;
    _offlineServerTenant = offlineServerTenant;
    _realtimeServerTenant = realtimeServerTenant;
  }

  public static QueryLogRecord from(QueryLogger.QueryLogParams params, String query, String clientIp,
      long logTimestampMs) {
    RequestContext requestContext = params.getRequestContext();
    BrokerResponse response = params.getResponse();
    return new QueryLogRecord(logTimestampMs, requestContext.getRequestId(), params.getTable(),
        requestContext.getBrokerId(), clientIp, query, params.getQueryEngine().getName(),
        requestContext.getRequestArrivalTimeMillis(), response.getTimeUsedMs(), response.getBrokerReduceTimeMs(),
        response.getNumDocsScanned(), response.getTotalDocs(), response.getNumEntriesScannedInFilter(),
        response.getNumEntriesScannedPostFilter(), response.getNumSegmentsQueried(), response.getNumSegmentsProcessed(),
        response.getNumSegmentsMatched(), response.getNumConsumingSegmentsQueried(),
        response.getNumConsumingSegmentsProcessed(), response.getNumConsumingSegmentsMatched(),
        requestContext.getNumUnavailableSegments(), response.getMinConsumingFreshnessTimeMs(),
        response.getNumServersResponded(), response.getNumServersQueried(), response.isGroupsTrimmed(),
        response.isNumGroupsLimitReached(), response.isNumGroupsWarningLimitReached(), response.getExceptionsSize(),
        formatExceptions(response.getExceptions()),
        params.getServerStats() != null ? params.getServerStats().getServerStats() : null,
        response.getOfflineTotalCpuTimeNs(), response.getOfflineThreadCpuTimeNs(),
        response.getOfflineSystemActivitiesCpuTimeNs(), response.getOfflineResponseSerializationCpuTimeNs(),
        response.getRealtimeTotalCpuTimeNs(), response.getRealtimeThreadCpuTimeNs(),
        response.getRealtimeSystemActivitiesCpuTimeNs(), response.getRealtimeResponseSerializationCpuTimeNs(),
        response.getOfflineTotalMemAllocatedBytes(), response.getOfflineThreadMemAllocatedBytes(),
        response.getOfflineResponseSerMemAllocatedBytes(), response.getRealtimeTotalMemAllocatedBytes(),
        response.getRealtimeThreadMemAllocatedBytes(), response.getRealtimeResponseSerMemAllocatedBytes(),
        toIntArray(response.getPools()), response.isPartialResult(), response.getRLSFiltersApplied(),
        response.getNumRowsResultSet(), toSortedArray(response.getTablesQueried()),
        toJson(response.getTraceInfo()), toFanoutName(requestContext.getFanoutType()),
        requestContext.getOfflineServerTenant(), requestContext.getRealtimeServerTenant());
  }

  private static String formatExceptions(List<QueryProcessingException> exceptions) {
    if (exceptions == null || exceptions.isEmpty()) {
      return "[]";
    }
    List<Map<String, Object>> serialized = new ArrayList<>(exceptions.size());
    for (QueryProcessingException exception : exceptions) {
      Map<String, Object> entry = new HashMap<>();
      entry.put("code", exception.getErrorCode());
      entry.put("message", exception.getMessage());
      serialized.add(entry);
    }
    try {
      return JsonUtils.objectToString(serialized);
    } catch (IOException e) {
      return serialized.toString();
    }
  }

  private static int[] toIntArray(@Nullable Set<Integer> values) {
    if (values == null || values.isEmpty()) {
      return new int[0];
    }
    return values.stream().sorted().mapToInt(Integer::intValue).toArray();
  }

  private static String[] toSortedArray(@Nullable Set<String> values) {
    if (values == null || values.isEmpty()) {
      return new String[0];
    }
    return values.stream().filter(Objects::nonNull).sorted(Comparator.naturalOrder()).toArray(String[]::new);
  }

  private static String toJson(@Nullable Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return "{}";
    }
    try {
      return JsonUtils.objectToString(map);
    } catch (IOException e) {
      return map.toString();
    }
  }

  private static String toFanoutName(@Nullable FanoutType fanoutType) {
    return fanoutType == null ? null : fanoutType.name();
  }

  public long getLogTimestampMs() {
    return _logTimestampMs;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public String getClientIp() {
    return _clientIp;
  }

  public String getQuery() {
    return _query;
  }

  public String getQueryEngine() {
    return _queryEngine;
  }

  public long getRequestArrivalTimeMs() {
    return _requestArrivalTimeMs;
  }

  public long getTimeMs() {
    return _timeMs;
  }

  public long getBrokerReduceTimeMs() {
    return _brokerReduceTimeMs;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  public long getNumSegmentsQueried() {
    return _numSegmentsQueried;
  }

  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  public long getNumConsumingSegmentsQueried() {
    return _numConsumingSegmentsQueried;
  }

  public long getNumConsumingSegmentsProcessed() {
    return _numConsumingSegmentsProcessed;
  }

  public long getNumConsumingSegmentsMatched() {
    return _numConsumingSegmentsMatched;
  }

  public long getNumUnavailableSegments() {
    return _numUnavailableSegments;
  }

  public long getMinConsumingFreshnessTimeMs() {
    return _minConsumingFreshnessTimeMs;
  }

  public int getNumServersResponded() {
    return _numServersResponded;
  }

  public int getNumServersQueried() {
    return _numServersQueried;
  }

  public boolean isGroupsTrimmed() {
    return _groupsTrimmed;
  }

  public boolean isGroupLimitReached() {
    return _groupLimitReached;
  }

  public boolean isGroupWarningLimitReached() {
    return _groupWarningLimitReached;
  }

  public long getNumExceptions() {
    return _numExceptions;
  }

  public String getExceptions() {
    return _exceptions;
  }

  public String getServerStats() {
    return _serverStats;
  }

  public long getOfflineTotalCpuTimeNs() {
    return _offlineTotalCpuTimeNs;
  }

  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  public long getRealtimeTotalCpuTimeNs() {
    return _realtimeTotalCpuTimeNs;
  }

  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  public long getOfflineTotalMemAllocatedBytes() {
    return _offlineTotalMemAllocatedBytes;
  }

  public long getOfflineThreadMemAllocatedBytes() {
    return _offlineThreadMemAllocatedBytes;
  }

  public long getOfflineResponseSerMemAllocatedBytes() {
    return _offlineResponseSerMemAllocatedBytes;
  }

  public long getRealtimeTotalMemAllocatedBytes() {
    return _realtimeTotalMemAllocatedBytes;
  }

  public long getRealtimeThreadMemAllocatedBytes() {
    return _realtimeThreadMemAllocatedBytes;
  }

  public long getRealtimeResponseSerMemAllocatedBytes() {
    return _realtimeResponseSerMemAllocatedBytes;
  }

  public int[] getPools() {
    return _pools;
  }

  public boolean isPartialResult() {
    return _partialResult;
  }

  public boolean isRlsFiltersApplied() {
    return _rlsFiltersApplied;
  }

  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  public String[] getTablesQueried() {
    return _tablesQueried;
  }

  public String getTraceInfoJson() {
    return _traceInfoJson;
  }

  public String getFanoutType() {
    return _fanoutType;
  }

  public String getOfflineServerTenant() {
    return _offlineServerTenant;
  }

  public String getRealtimeServerTenant() {
    return _realtimeServerTenant;
  }
}
