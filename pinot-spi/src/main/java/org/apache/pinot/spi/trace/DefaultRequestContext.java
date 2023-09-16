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

import java.util.concurrent.TimeUnit;


/**
 * A class to hold the details regarding a request and the statistics.
 * This object can be used to publish the query processing statistics to a stream for
 * post-processing at a finer level than metrics.
 */
public class DefaultRequestContext implements RequestScope {

  private static final String DEFAULT_TABLE_NAME = "NotYetParsed";

  private int _errorCode = 0;
  private String _query;
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

  public DefaultRequestContext() {
  }

  @Override
  public RequestContext getRequestContext() {
    return this;
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  @Override
  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  @Override
  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  @Override
  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  @Override
  public long getOfflineTotalCpuTimeNs() {
    return _offlineTotalCpuTimeNs;
  }

  @Override
  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  @Override
  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return _realtimeTotalCpuTimeNs;
  }

  @Override
  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
  }

  @Override
  public String getBrokerId() {
    return _brokerId;
  }

  @Override
  public String getOfflineServerTenant() {
    return _offlineServerTenant;
  }

  @Override
  public String getRealtimeServerTenant() {
    return _realtimeServerTenant;
  }

  @Override
  public long getRequestId() {
    return _requestId;
  }

  @Override
  public long getRequestArrivalTimeMillis() {
    return _requestArrivalTimeMillis;
  }

  @Override
  public long getReduceTimeMillis() {
    return _reduceTimeMillis;
  }

  @Override
  public void setErrorCode(int errorCode) {
    _errorCode = errorCode;
  }

  @Override
  public void setQuery(String query) {
    _query = query;
  }

  @Override
  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  @Override
  public void setQueryProcessingTime(long processingTimeMillis) {
    _processingTimeMillis = processingTimeMillis;
  }

  @Override
  public void setBrokerId(String brokerId) {
    _brokerId = brokerId;
  }

  @Override
  public void setOfflineServerTenant(String offlineServerTenant) {
    _offlineServerTenant = offlineServerTenant;
  }

  @Override
  public void setRealtimeServerTenant(String realtimeServerTenant) {
    _realtimeServerTenant = realtimeServerTenant;
  }

  @Override
  public void setRequestId(long requestId) {
    _requestId = requestId;
  }

  @Override
  public void setRequestArrivalTimeMillis(long requestArrivalTimeMillis) {
    _requestArrivalTimeMillis = requestArrivalTimeMillis;
  }

  @Override
  public void setReduceTimeNanos(long reduceTimeNanos) {
    _reduceTimeMillis = TimeUnit.MILLISECONDS.convert(reduceTimeNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void setFanoutType(FanoutType fanoutType) {
    _fanoutType = fanoutType;
  }

  @Override
  public FanoutType getFanoutType() {
    return _fanoutType;
  }

  @Override
  public void setNumUnavailableSegments(int numUnavailableSegments) {
    _numUnavailableSegments = numUnavailableSegments;
  }

  @Override
  public int getNumUnavailableSegments() {
    return _numUnavailableSegments;
  }

  @Override
  public int getErrorCode() {
    return _errorCode;
  }

  @Override
  public String getQuery() {
    return _query;
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public long getProcessingTimeMillis() {
    return _processingTimeMillis;
  }

  @Override
  public long getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  @Override
  public long getNumSegmentsQueried() {
    return _numSegmentsQueried;
  }

  @Override
  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  @Override
  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  @Override
  public int getNumServersQueried() {
    return _numServersQueried;
  }

  @Override
  public int getNumServersResponded() {
    return _numServersResponded;
  }

  @Override
  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _isNumGroupsLimitReached;
  }

  @Override
  public int getNumExceptions() {
    return _numExceptions;
  }

  @Override
  public boolean hasValidTableName() {
    return !DEFAULT_TABLE_NAME.equals(_tableName);
  }

  @Override
  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  @Override
  public void setProcessingTimeMillis(long processingTimeMillis) {
    _processingTimeMillis = processingTimeMillis;
  }

  @Override
  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  @Override
  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @Override
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @Override
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  @Override
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _numSegmentsQueried = numSegmentsQueried;
  }

  @Override
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @Override
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  @Override
  public void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs) {
    _offlineThreadCpuTimeNs = offlineThreadCpuTimeNs;
  }

  @Override
  public void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs) {
    _realtimeThreadCpuTimeNs = realtimeThreadCpuTimeNs;
  }

  @Override
  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  @Override
  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  @Override
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _isNumGroupsLimitReached = numGroupsLimitReached;
  }

  @Override
  public void setNumExceptions(int numExceptions) {
    _numExceptions = numExceptions;
  }

  @Override
  public void setNumRowsResultSet(int numRowsResultSet) {
    _numRowsResultSet = numRowsResultSet;
  }

  @Override
  public void setReduceTimeMillis(long reduceTimeMillis) {
    _reduceTimeMillis = reduceTimeMillis;
  }

  @Override
  public void close() {
  }
}
