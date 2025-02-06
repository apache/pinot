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


public class MdcRequestScope implements RequestScope {
  private final RequestScope _delegate;

  public MdcRequestScope(RequestScope delegate) {
    _delegate = delegate;
    LoggerConstants.COMPONENT_TYPE_KEY.registerOnMdc("broker");
  }

  @Override
  public void close() {
    _delegate.close();

    LoggerConstants.QUERY_ID_KEY.unregisterFromMdc();
    LoggerConstants.COMPONENT_ID_KEY.unregisterFromMdc();
    LoggerConstants.COMPONENT_TYPE_KEY.unregisterFromMdc();
  }

  @Override
  public void close(Object context) {
    _delegate.close(context);

    LoggerConstants.QUERY_ID_KEY.unregisterFromMdc();
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _delegate.getOfflineSystemActivitiesCpuTimeNs();
  }

  @Override
  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _delegate.setOfflineSystemActivitiesCpuTimeNs(offlineSystemActivitiesCpuTimeNs);
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _delegate.getRealtimeSystemActivitiesCpuTimeNs();
  }

  @Override
  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _delegate.setRealtimeSystemActivitiesCpuTimeNs(realtimeSystemActivitiesCpuTimeNs);
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return _delegate.getOfflineResponseSerializationCpuTimeNs();
  }

  @Override
  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _delegate.setOfflineResponseSerializationCpuTimeNs(offlineResponseSerializationCpuTimeNs);
  }

  @Override
  public long getOfflineTotalCpuTimeNs() {
    return _delegate.getOfflineTotalCpuTimeNs();
  }

  @Override
  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    _delegate.setOfflineTotalCpuTimeNs(offlineTotalCpuTimeNs);
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _delegate.getRealtimeResponseSerializationCpuTimeNs();
  }

  @Override
  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _delegate.setRealtimeResponseSerializationCpuTimeNs(realtimeResponseSerializationCpuTimeNs);
  }

  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return _delegate.getRealtimeTotalCpuTimeNs();
  }

  @Override
  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    _delegate.setRealtimeTotalCpuTimeNs(realtimeTotalCpuTimeNs);
  }

  @Override
  public String getBrokerId() {
    return _delegate.getBrokerId();
  }

  @Override
  public String getOfflineServerTenant() {
    return _delegate.getOfflineServerTenant();
  }

  @Override
  public String getRealtimeServerTenant() {
    return _delegate.getRealtimeServerTenant();
  }

  @Override
  public long getRequestId() {
    return _delegate.getRequestId();
  }

  @Override
  public boolean isSampledRequest() {
    return _delegate.isSampledRequest();
  }

  @Override
  public long getRequestArrivalTimeMillis() {
    return _delegate.getRequestArrivalTimeMillis();
  }

  @Override
  public long getReduceTimeMillis() {
    return _delegate.getReduceTimeMillis();
  }

  @Override
  public void setErrorCode(int errorCode) {
    _delegate.setErrorCode(errorCode);
  }

  @Override
  public void setQuery(String pql) {
    _delegate.setQuery(pql);
  }

  @Override
  public void setTableName(String tableName) {
    _delegate.setTableName(tableName);
  }

  @Override
  public void setTableNames(List<String> tableNames) {
    _delegate.setTableNames(tableNames);
  }

  @Override
  public void setQueryProcessingTime(long processingTimeMillis) {
    _delegate.setQueryProcessingTime(processingTimeMillis);
  }

  @Override
  public void setBrokerId(String brokerId) {
    LoggerConstants.COMPONENT_ID_KEY.registerOnMdc(brokerId);
    _delegate.setBrokerId(brokerId);
  }

  @Override
  public void setOfflineServerTenant(String offlineServerTenant) {
    _delegate.setOfflineServerTenant(offlineServerTenant);
  }

  @Override
  public void setRealtimeServerTenant(String realtimeServerTenant) {
    _delegate.setRealtimeServerTenant(realtimeServerTenant);
  }

  @Override
  public void setRequestId(long requestId) {
    LoggerConstants.QUERY_ID_KEY.registerOnMdc(String.valueOf(requestId));
    _delegate.setRequestId(requestId);
  }

  @Override
  public void setRequestArrivalTimeMillis(long requestArrivalTimeMillis) {
    _delegate.setRequestArrivalTimeMillis(requestArrivalTimeMillis);
  }

  @Override
  public void setReduceTimeNanos(long reduceTimeNanos) {
    _delegate.setReduceTimeNanos(reduceTimeNanos);
  }

  @Override
  public void setFanoutType(FanoutType fanoutType) {
    _delegate.setFanoutType(fanoutType);
  }

  @Override
  public FanoutType getFanoutType() {
    return _delegate.getFanoutType();
  }

  @Override
  public void setNumUnavailableSegments(int numUnavailableSegments) {
    _delegate.setNumUnavailableSegments(numUnavailableSegments);
  }

  @Override
  public int getNumUnavailableSegments() {
    return _delegate.getNumUnavailableSegments();
  }

  @Override
  public int getErrorCode() {
    return _delegate.getErrorCode();
  }

  @Override
  public String getQuery() {
    return _delegate.getQuery();
  }

  @Override
  public String getTableName() {
    return _delegate.getTableName();
  }

  @Override
  public List<String> getTableNames() {
    return _delegate.getTableNames();
  }

  @Override
  public long getProcessingTimeMillis() {
    return _delegate.getProcessingTimeMillis();
  }

  @Override
  public long getTotalDocs() {
    return _delegate.getTotalDocs();
  }

  @Override
  public long getNumDocsScanned() {
    return _delegate.getNumDocsScanned();
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _delegate.getNumEntriesScannedInFilter();
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    return _delegate.getNumEntriesScannedPostFilter();
  }

  @Override
  public long getNumSegmentsQueried() {
    return _delegate.getNumSegmentsQueried();
  }

  @Override
  public long getNumSegmentsProcessed() {
    return _delegate.getNumSegmentsProcessed();
  }

  @Override
  public long getNumSegmentsMatched() {
    return _delegate.getNumSegmentsMatched();
  }

  @Override
  public int getNumServersQueried() {
    return _delegate.getNumServersQueried();
  }

  @Override
  public int getNumServersResponded() {
    return _delegate.getNumServersResponded();
  }

  @Override
  public long getOfflineThreadCpuTimeNs() {
    return _delegate.getOfflineThreadCpuTimeNs();
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return _delegate.getRealtimeThreadCpuTimeNs();
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _delegate.isNumGroupsLimitReached();
  }

  @Override
  public int getNumExceptions() {
    return _delegate.getNumExceptions();
  }

  @Override
  public boolean hasValidTableName() {
    return _delegate.hasValidTableName();
  }

  @Override
  public int getNumRowsResultSet() {
    return _delegate.getNumRowsResultSet();
  }

  @Override
  public void setProcessingTimeMillis(long processingTimeMillis) {
    _delegate.setProcessingTimeMillis(processingTimeMillis);
  }

  @Override
  public void setTotalDocs(long totalDocs) {
    _delegate.setTotalDocs(totalDocs);
  }

  @Override
  public void setNumDocsScanned(long numDocsScanned) {
    _delegate.setNumDocsScanned(numDocsScanned);
  }

  @Override
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _delegate.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
  }

  @Override
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _delegate.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
  }

  @Override
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _delegate.setNumSegmentsQueried(numSegmentsQueried);
  }

  @Override
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _delegate.setNumSegmentsProcessed(numSegmentsProcessed);
  }

  @Override
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _delegate.setNumSegmentsMatched(numSegmentsMatched);
  }

  @Override
  public void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs) {
    _delegate.setOfflineThreadCpuTimeNs(offlineThreadCpuTimeNs);
  }

  @Override
  public void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs) {
    _delegate.setRealtimeThreadCpuTimeNs(realtimeThreadCpuTimeNs);
  }

  @Override
  public void setNumServersQueried(int numServersQueried) {
    _delegate.setNumServersQueried(numServersQueried);
  }

  @Override
  public void setNumServersResponded(int numServersResponded) {
    _delegate.setNumServersResponded(numServersResponded);
  }

  @Override
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _delegate.setNumGroupsLimitReached(numGroupsLimitReached);
  }

  @Override
  public void setNumExceptions(int numExceptions) {
    _delegate.setNumExceptions(numExceptions);
  }

  @Override
  public void setNumRowsResultSet(int numRowsResultSet) {
    _delegate.setNumRowsResultSet(numRowsResultSet);
  }

  @Override
  public void setReduceTimeMillis(long reduceTimeMillis) {
    _delegate.setReduceTimeMillis(reduceTimeMillis);
  }

  @Override
  public long getNumConsumingSegmentsQueried() {
    return _delegate.getNumConsumingSegmentsQueried();
  }

  @Override
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _delegate.setNumConsumingSegmentsQueried(numConsumingSegmentsQueried);
  }

  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _delegate.getNumConsumingSegmentsProcessed();
  }

  @Override
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _delegate.setNumConsumingSegmentsProcessed(numConsumingSegmentsProcessed);
  }

  @Override
  public long getNumConsumingSegmentsMatched() {
    return _delegate.getNumConsumingSegmentsMatched();
  }

  @Override
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _delegate.setNumConsumingSegmentsMatched(numConsumingSegmentsMatched);
  }

  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _delegate.getMinConsumingFreshnessTimeMs();
  }

  @Override
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _delegate.setMinConsumingFreshnessTimeMs(minConsumingFreshnessTimeMs);
  }

  @Override
  public long getNumSegmentsPrunedByBroker() {
    return _delegate.getNumSegmentsPrunedByBroker();
  }

  @Override
  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _delegate.setNumSegmentsPrunedByBroker(numSegmentsPrunedByBroker);
  }

  @Override
  public long getNumSegmentsPrunedByServer() {
    return _delegate.getNumSegmentsPrunedByServer();
  }

  @Override
  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _delegate.setNumSegmentsPrunedByServer(numSegmentsPrunedByServer);
  }

  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _delegate.getNumSegmentsPrunedInvalid();
  }

  @Override
  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _delegate.setNumSegmentsPrunedInvalid(numSegmentsPrunedInvalid);
  }

  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _delegate.getNumSegmentsPrunedByLimit();
  }

  @Override
  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _delegate.setNumSegmentsPrunedByLimit(numSegmentsPrunedByLimit);
  }

  @Override
  public long getNumSegmentsPrunedByValue() {
    return _delegate.getNumSegmentsPrunedByValue();
  }

  @Override
  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _delegate.setNumSegmentsPrunedByValue(numSegmentsPrunedByValue);
  }

  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return _delegate.getExplainPlanNumEmptyFilterSegments();
  }

  @Override
  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _delegate.setExplainPlanNumEmptyFilterSegments(explainPlanNumEmptyFilterSegments);
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _delegate.getExplainPlanNumMatchAllFilterSegments();
  }

  @Override
  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _delegate.setExplainPlanNumMatchAllFilterSegments(explainPlanNumMatchAllFilterSegments);
  }

  @Override
  public Map<String, String> getTraceInfo() {
    return _delegate.getTraceInfo();
  }

  @Override
  public void setTraceInfo(Map<String, String> traceInfo) {
    _delegate.setTraceInfo(traceInfo);
  }

  @Override
  public List<String> getProcessingExceptions() {
    return _delegate.getProcessingExceptions();
  }

  @Override
  public void setProcessingExceptions(List<String> processingExceptions) {
    _delegate.setProcessingExceptions(processingExceptions);
  }

  @Override
  public Map<String, List<String>> getRequestHttpHeaders() {
    return _delegate.getRequestHttpHeaders();
  }

  @Override
  public void setRequestHttpHeaders(Map<String, List<String>> requestHttpHeaders) {
    _delegate.setRequestHttpHeaders(requestHttpHeaders);
  }
}
