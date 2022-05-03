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
package org.apache.pinot.broker.api;

import java.util.concurrent.TimeUnit;


/**
 * A class to hold the details regarding a request and the statistics.
 * This object can be used to publish the query processing statistics to a stream for
 * post-processing at a finer level than metrics.
 */
public class RequestStatistics {

  public enum FanoutType {
    OFFLINE, REALTIME, HYBRID
  }

  private static final String DEFAULT_TABLE_NAME = "NotYetParsed";

  private int _errorCode = 0;
  private String _pql;
  private String _tableName = DEFAULT_TABLE_NAME;
  private long _processingTimeMillis = -1;
  private long _totalDocs;
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private long _numSegmentsQueried;
  private long _numSegmentsProcessed;
  private long _numSegmentsMatched;
  private long _offlineThreadCpuTimeNs;
  private long _realtimeThreadCpuTimeNs;
  private long _offlineSystemActivitiesCpuTimeNs;
  private long _realtimeSystemActivitiesCpuTimeNs;
  private long _offlineResponseSerializationCpuTimeNs;
  private long _realtimeResponseSerializationCpuTimeNs;
  private long _offlineTotalCpuTimeNs;
  private long _realtimeTotalCpuTimeNs;
  private int _numServersQueried;
  private int _numServersResponded;
  private boolean _isNumGroupsLimitReached;
  private int _numExceptions;
  private String _brokerId;
  private String _offlineServerTenant;
  private String _realtimeServerTenant;
  private long _requestId;
  private int _numRowsResultSet;
  private long _requestArrivalTimeMillis;
  private long _reduceTimeMillis;

  private FanoutType _fanoutType;
  private int _numUnavailableSegments;

  public RequestStatistics() {
  }

  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  public long getOfflineTotalCpuTimeNs() {
    return _offlineTotalCpuTimeNs;
  }

  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
  }

  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  public long getRealtimeTotalCpuTimeNs() {
    return _realtimeTotalCpuTimeNs;
  }

  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public String getOfflineServerTenant() {
    return _offlineServerTenant;
  }

  public String getRealtimeServerTenant() {
    return _realtimeServerTenant;
  }

  public long getRequestId() {
    return _requestId;
  }

  public long getRequestArrivalTimeMillis() {
    return _requestArrivalTimeMillis;
  }

  public long getReduceTimeMillis() {
    return _reduceTimeMillis;
  }

  public void setErrorCode(int errorCode) {
    _errorCode = errorCode;
  }

  public void setPql(String pql) {
    _pql = pql;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public void setQueryProcessingTime(long processingTimeMillis) {
    _processingTimeMillis = processingTimeMillis;
  }

  public void setBrokerId(String brokerId) {
    _brokerId = brokerId;
  }

  public void setOfflineServerTenant(String offlineServerTenant) {
    _offlineServerTenant = offlineServerTenant;
  }

  public void setRealtimeServerTenant(String realtimeServerTenant) {
    _realtimeServerTenant = realtimeServerTenant;
  }

  public void setRequestId(long requestId) {
    _requestId = requestId;
  }

  public void setRequestArrivalTimeMillis(long requestArrivalTimeMillis) {
    _requestArrivalTimeMillis = requestArrivalTimeMillis;
  }

  public void setReduceTimeNanos(long reduceTimeNanos) {
    _reduceTimeMillis = TimeUnit.MILLISECONDS.convert(reduceTimeNanos, TimeUnit.NANOSECONDS);
  }

  public void setFanoutType(FanoutType fanoutType) {
    _fanoutType = fanoutType;
  }

  public FanoutType getFanoutType() {
    return _fanoutType;
  }

  public void setNumUnavailableSegments(int numUnavailableSegments) {
    _numUnavailableSegments = numUnavailableSegments;
  }

  public int getNumUnavailableSegments() {
    return _numUnavailableSegments;
  }

  public int getErrorCode() {
    return _errorCode;
  }

  public String getPql() {
    return _pql;
  }

  public String getTableName() {
    return _tableName;
  }

  public long getProcessingTimeMillis() {
    return _processingTimeMillis;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
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

  public int getNumServersQueried() {
    return _numServersQueried;
  }

  public int getNumServersResponded() {
    return _numServersResponded;
  }

  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  public boolean isNumGroupsLimitReached() {
    return _isNumGroupsLimitReached;
  }

  public int getNumExceptions() {
    return _numExceptions;
  }

  public boolean hasValidTableName() {
    return !DEFAULT_TABLE_NAME.equals(_tableName);
  }

  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  public void setProcessingTimeMillis(long processingTimeMillis) {
    _processingTimeMillis = processingTimeMillis;
  }

  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _numSegmentsQueried = numSegmentsQueried;
  }

  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  public void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs) {
    _offlineThreadCpuTimeNs = offlineThreadCpuTimeNs;
  }

  public void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs) {
    _realtimeThreadCpuTimeNs = realtimeThreadCpuTimeNs;
  }

  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _isNumGroupsLimitReached = numGroupsLimitReached;
  }

  public void setNumExceptions(int numExceptions) {
    _numExceptions = numExceptions;
  }

  public void setNumRowsResultSet(int numRowsResultSet) {
    _numRowsResultSet = numRowsResultSet;
  }

  public void setReduceTimeMillis(long reduceTimeMillis) {
    _reduceTimeMillis = reduceTimeMillis;
  }
}
