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
package org.apache.pinot.spi.trace;

import java.util.List;
import java.util.Map;


public interface RequestContext {
  long getOfflineSystemActivitiesCpuTimeNs();

  void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs);

  long getRealtimeSystemActivitiesCpuTimeNs();

  void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs);

  long getOfflineResponseSerializationCpuTimeNs();

  void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs);

  long getOfflineTotalCpuTimeNs();

  void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs);

  long getRealtimeResponseSerializationCpuTimeNs();

  void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs);

  long getRealtimeTotalCpuTimeNs();

  void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs);

  String getBrokerId();

  String getOfflineServerTenant();

  String getRealtimeServerTenant();

  long getRequestId();

  default boolean isSampledRequest() {
    return false;
  }

  long getRequestArrivalTimeMillis();

  long getReduceTimeMillis();

  void setErrorCode(int errorCode);

  void setQuery(String pql);

  void setTableName(String tableName);

  void setTableNames(List<String> tableNames);

  void setQueryProcessingTime(long processingTimeMillis);

  void setBrokerId(String brokerId);

  void setOfflineServerTenant(String offlineServerTenant);

  void setRealtimeServerTenant(String realtimeServerTenant);

  void setRequestId(long requestId);

  void setRequestArrivalTimeMillis(long requestArrivalTimeMillis);

  void setReduceTimeNanos(long reduceTimeNanos);

  void setFanoutType(FanoutType fanoutType);

  FanoutType getFanoutType();

  void setNumUnavailableSegments(int numUnavailableSegments);

  int getNumUnavailableSegments();

  int getErrorCode();

  String getQuery();

  String getTableName();

  List<String> getTableNames();

  long getProcessingTimeMillis();

  long getTotalDocs();

  long getNumDocsScanned();

  long getNumEntriesScannedInFilter();

  long getNumEntriesScannedPostFilter();

  long getNumSegmentsQueried();

  long getNumSegmentsProcessed();

  long getNumSegmentsMatched();

  int getNumServersQueried();

  int getNumServersResponded();

  long getOfflineThreadCpuTimeNs();

  long getRealtimeThreadCpuTimeNs();

  boolean isNumGroupsLimitReached();

  int getNumExceptions();

  boolean hasValidTableName();

  int getNumRowsResultSet();

  void setProcessingTimeMillis(long processingTimeMillis);

  void setTotalDocs(long totalDocs);

  void setNumDocsScanned(long numDocsScanned);

  void setNumEntriesScannedInFilter(long numEntriesScannedInFilter);

  void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter);

  void setNumSegmentsQueried(long numSegmentsQueried);

  void setNumSegmentsProcessed(long numSegmentsProcessed);

  void setNumSegmentsMatched(long numSegmentsMatched);

  void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs);

  void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs);

  void setNumServersQueried(int numServersQueried);

  void setNumServersResponded(int numServersResponded);

  void setNumGroupsLimitReached(boolean numGroupsLimitReached);

  void setNumExceptions(int numExceptions);

  void setNumRowsResultSet(int numRowsResultSet);

  void setReduceTimeMillis(long reduceTimeMillis);

  long getNumConsumingSegmentsQueried();

  void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried);

  long getNumConsumingSegmentsProcessed();

  void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed);

  long getNumConsumingSegmentsMatched();

  void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched);

  long getMinConsumingFreshnessTimeMs();

  void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs);

  long getNumSegmentsPrunedByBroker();

  void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker);

  long getNumSegmentsPrunedByServer();

  void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer);

  long getNumSegmentsPrunedInvalid();

  void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid);

  long getNumSegmentsPrunedByLimit();

  void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit);

  long getNumSegmentsPrunedByValue();

  void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue);

  long getExplainPlanNumEmptyFilterSegments();

  void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments);

  long getExplainPlanNumMatchAllFilterSegments();

  void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments);

  Map<String, String> getTraceInfo();

  void setTraceInfo(Map<String, String> traceInfo);

  List<String> getProcessingExceptions();

  void setProcessingExceptions(List<String> processingExceptions);

  Map<String, List<String>> getRequestHttpHeaders();

  void setRequestHttpHeaders(Map<String, List<String>> requestHttpHeaders);

  enum FanoutType {
    OFFLINE, REALTIME, HYBRID
  }
}
