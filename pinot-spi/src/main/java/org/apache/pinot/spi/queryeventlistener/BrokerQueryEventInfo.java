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
package org.apache.pinot.spi.queryeventlistener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.trace.RequestContext;

public class BrokerQueryEventInfo {
    private String _requestId;
    private String _brokerId;
    private String _query;
    private String _queryStatus;
    private String _failureJson;
    private int _errorCode;

    private long _requestArrivalTimeMillis;
    private int _numServersQueried;
    private int _numServersResponded = 0;
    private long _numDocsScanned = 0L;
    private long _numEntriesScannedInFilter = 0L;
    private long _numEntriesScannedPostFilter = 0L;
    private long _numSegmentsQueried = 0L;
    private long _numSegmentsProcessed = 0L;
    private long _numSegmentsMatched = 0L;
    private long _numConsumingSegmentsQueried = 0L;
    private long _numConsumingSegmentsProcessed = 0L;
    private long _numConsumingSegmentsMatched = 0L;

    private long _minConsumingFreshnessTimeMs = 0L;

    private long _totalDocs = 0L;
    private boolean _numGroupsLimitReached = false;
    private long _timeUsedMs = 0L;
    private long _offlineThreadCpuTimeNs = 0L;
    private long _realtimeThreadCpuTimeNs = 0L;
    private long _offlineSystemActivitiesCpuTimeNs = 0L;
    private long _realtimeSystemActivitiesCpuTimeNs = 0L;
    private long _offlineResponseSerializationCpuTimeNs = 0L;
    private long _realtimeResponseSerializationCpuTimeNs = 0L;
    private long _offlineTotalCpuTimeNs = 0L;
    private long _realtimeTotalCpuTimeNs = 0L;
    private long _numSegmentsPrunedByBroker = 0L;
    private long _numSegmentsPrunedByServer = 0L;
    private long _numSegmentsPrunedInvalid = 0L;
    private long _numSegmentsPrunedByLimit = 0L;
    private long _numSegmentsPrunedByValue = 0L;
    private long _explainPlanNumEmptyFilterSegments = 0L;
    private long _explainPlanNumMatchAllFilterSegments = 0L;
    private int _numRowsResultSet = 0;
    private List<String> _tableNames = new ArrayList<>();
    private String _offlineServerTenant;
    private String _realtimeServerTenant;

    public BrokerQueryEventInfo() {
    }

    public BrokerQueryEventInfo(RequestContext requestContext) {
        _requestId = String.valueOf(requestContext.getRequestId());
        _brokerId = requestContext.getBrokerId();
        _query = requestContext.getQuery();
        _errorCode = requestContext.getErrorCode();
        _requestArrivalTimeMillis = requestContext.getRequestArrivalTimeMillis();
        _numServersQueried = requestContext.getNumServersQueried();
        _numServersResponded = requestContext.getNumServersResponded();
        _numDocsScanned = requestContext.getNumDocsScanned();
        _numEntriesScannedPostFilter = requestContext.getNumEntriesScannedInFilter();
        _numEntriesScannedPostFilter = requestContext.getNumEntriesScannedPostFilter();
        _numSegmentsQueried = requestContext.getNumSegmentsQueried();
        _numSegmentsProcessed = requestContext.getNumSegmentsProcessed();
        _numSegmentsMatched = requestContext.getNumSegmentsMatched();
        // TODO add consuming segments stats, timeUsedMs
        _totalDocs = requestContext.getTotalDocs();
        _numGroupsLimitReached = requestContext.isNumGroupsLimitReached();
        _offlineThreadCpuTimeNs = requestContext.getOfflineThreadCpuTimeNs();
        _realtimeThreadCpuTimeNs = requestContext.getRealtimeThreadCpuTimeNs();
        _offlineSystemActivitiesCpuTimeNs = requestContext.getOfflineSystemActivitiesCpuTimeNs();
        _realtimeSystemActivitiesCpuTimeNs = requestContext.getRealtimeSystemActivitiesCpuTimeNs();
        _offlineResponseSerializationCpuTimeNs = requestContext.getOfflineResponseSerializationCpuTimeNs();
        _realtimeSystemActivitiesCpuTimeNs = requestContext.getRealtimeResponseSerializationCpuTimeNs();
        _offlineTotalCpuTimeNs = requestContext.getOfflineTotalCpuTimeNs();
        _realtimeTotalCpuTimeNs = requestContext.getRealtimeTotalCpuTimeNs();
        _numRowsResultSet = requestContext.getNumRowsResultSet();
        _tableNames = new ArrayList<>(Collections.singleton(requestContext.getTableName()));
        _offlineServerTenant = requestContext.getOfflineServerTenant();
        _realtimeServerTenant = requestContext.getRealtimeServerTenant();
    }

    public String getRequestId() {
        return _requestId;
    }

    public void setRequestId(String requestId) {
        _requestId = requestId;
    }

    public String getBrokerId() {
        return _brokerId;
    }

    public void setBrokerId(String brokerId) {
        _brokerId = brokerId;
    }

    public String getQuery() {
        return _query;
    }

    public void setQuery(String query) {
        _query = query;
    }

    public String getQueryStatus() {
        return _queryStatus;
    }

    public void setQueryStatus(String queryStatus) {
        _queryStatus = queryStatus;
    }

    public String getFailureJson() {
        return _failureJson;
    }

    public void setFailureJson(String failureJson) {
        _failureJson = failureJson;
    }

    public int getErrorCode() {
        return _errorCode;
    }

    public void setErrorCode(int errorCode) {
        _errorCode = errorCode;
    }

    public long getRequestArrivalTimeMillis() {
        return _requestArrivalTimeMillis;
    }

    public void setRequestArrivalTimeMillis(long requestArrivalTimeMillis) {
        _requestArrivalTimeMillis = requestArrivalTimeMillis;
    }

    public int getNumServersQueried() {
        return _numServersQueried;
    }

    public void setNumServersQueried(int numServersQueried) {
        _numServersQueried = numServersQueried;
    }

    public int getNumServersResponded() {
        return _numServersResponded;
    }

    public void setNumServersResponded(int numServersResponded) {
        _numServersResponded = numServersResponded;
    }

    public long getNumDocsScanned() {
        return _numDocsScanned;
    }

    public void setNumDocsScanned(long numDocsScanned) {
        _numDocsScanned = numDocsScanned;
    }

    public long getNumEntriesScannedInFilter() {
        return _numEntriesScannedInFilter;
    }

    public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
        _numEntriesScannedInFilter = numEntriesScannedInFilter;
    }

    public long getNumEntriesScannedPostFilter() {
        return _numEntriesScannedPostFilter;
    }

    public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
        _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    }

    public long getNumSegmentsQueried() {
        return _numSegmentsQueried;
    }

    public void setNumSegmentsQueried(long numSegmentsQueried) {
        _numSegmentsQueried = numSegmentsQueried;
    }

    public long getNumSegmentsProcessed() {
        return _numSegmentsProcessed;
    }

    public void setNumSegmentsProcessed(long numSegmentsProcessed) {
        _numSegmentsProcessed = numSegmentsProcessed;
    }

    public long getNumSegmentsMatched() {
        return _numSegmentsMatched;
    }

    public void setNumSegmentsMatched(long numSegmentsMatched) {
        _numSegmentsMatched = numSegmentsMatched;
    }

    public long getNumConsumingSegmentsQueried() {
        return _numConsumingSegmentsQueried;
    }

    public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
        _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
    }

    public long getNumConsumingSegmentsProcessed() {
        return _numConsumingSegmentsProcessed;
    }

    public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
        _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
    }

    public long getNumConsumingSegmentsMatched() {
        return _numConsumingSegmentsMatched;
    }

    public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
        _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
    }

    public long getMinConsumingFreshnessTimeMs() {
        return _minConsumingFreshnessTimeMs;
    }

    public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
        _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
    }

    public long getTotalDocs() {
        return _totalDocs;
    }

    public void setTotalDocs(long totalDocs) {
        _totalDocs = totalDocs;
    }

    public boolean isNumGroupsLimitReached() {
        return _numGroupsLimitReached;
    }

    public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
        _numGroupsLimitReached = numGroupsLimitReached;
    }

    public long getTimeUsedMs() {
        return _timeUsedMs;
    }

    public void setTimeUsedMs(long timeUsedMs) {
        _timeUsedMs = timeUsedMs;
    }

    public long getOfflineThreadCpuTimeNs() {
        return _offlineThreadCpuTimeNs;
    }

    public void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs) {
        _offlineThreadCpuTimeNs = offlineThreadCpuTimeNs;
    }

    public long getRealtimeThreadCpuTimeNs() {
        return _realtimeThreadCpuTimeNs;
    }

    public void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs) {
        _realtimeThreadCpuTimeNs = realtimeThreadCpuTimeNs;
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

    public long getRealtimeResponseSerializationCpuTimeNs() {
        return _realtimeResponseSerializationCpuTimeNs;
    }

    public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
        _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
    }

    public long getOfflineTotalCpuTimeNs() {
        return _offlineTotalCpuTimeNs;
    }

    public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
        _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
    }

    public long getRealtimeTotalCpuTimeNs() {
        return _realtimeTotalCpuTimeNs;
    }

    public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
        _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
    }

    public long getNumSegmentsPrunedByBroker() {
        return _numSegmentsPrunedByBroker;
    }

    public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
        _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
    }

    public long getNumSegmentsPrunedByServer() {
        return _numSegmentsPrunedByServer;
    }

    public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
        _numSegmentsPrunedByServer = numSegmentsPrunedByServer;
    }

    public long getNumSegmentsPrunedInvalid() {
        return _numSegmentsPrunedInvalid;
    }

    public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
        _numSegmentsPrunedInvalid = numSegmentsPrunedInvalid;
    }

    public long getNumSegmentsPrunedByLimit() {
        return _numSegmentsPrunedByLimit;
    }

    public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
        _numSegmentsPrunedByLimit = numSegmentsPrunedByLimit;
    }

    public long getNumSegmentsPrunedByValue() {
        return _numSegmentsPrunedByValue;
    }

    public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
        _numSegmentsPrunedByValue = numSegmentsPrunedByValue;
    }

    public long getExplainPlanNumEmptyFilterSegments() {
        return _explainPlanNumEmptyFilterSegments;
    }

    public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
        _explainPlanNumEmptyFilterSegments = explainPlanNumEmptyFilterSegments;
    }

    public long getExplainPlanNumMatchAllFilterSegments() {
        return _explainPlanNumMatchAllFilterSegments;
    }

    public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
        _explainPlanNumMatchAllFilterSegments = explainPlanNumMatchAllFilterSegments;
    }

    public int getNumRowsResultSet() {
        return _numRowsResultSet;
    }

    public void setNumRowsResultSet(int numRowsResultSet) {
        _numRowsResultSet = numRowsResultSet;
    }

    public List<String> getTableNames() {
        return _tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        _tableNames = tableNames;
    }

    public String getOfflineServerTenant() {
        return _offlineServerTenant;
    }

    public void setOffLineServerTenant(String offLineServerTenant) {
        _offlineServerTenant = offLineServerTenant;
    }

    public String getRealtimeServerTenant() {
        return _realtimeServerTenant;
    }

    public void setRealtimeServerTenant(String realtimeServerTenant) {
        _realtimeServerTenant = realtimeServerTenant;
    }

    @Override
    public String toString() {
        return "BrokerQueryEventInfo{" + "_requestId='" + _requestId + '\'' + ", _brokerId='" + _brokerId + '\''
                + ", _query='" + _query + '\'' + ", _queryStatus='" + _queryStatus + '\'' + ", _failureJson='"
                + _failureJson + '\'' + ", _errorCode=" + _errorCode + ", _requestArrivalTimeMillis="
                + _requestArrivalTimeMillis + ", _numServersQueried=" + _numServersQueried
                + ", _numServersResponded=" + _numServersResponded + ", _numDocsScanned=" + _numDocsScanned
                + ", _numEntriesScannedInFilter=" + _numEntriesScannedInFilter + ", _numEntriesScannedPostFilter="
                + _numEntriesScannedPostFilter + ", _numSegmentsQueried=" + _numSegmentsQueried
                + ", _numSegmentsProcessed=" + _numSegmentsProcessed + ", _numSegmentsMatched=" + _numSegmentsMatched
                + ", _numConsumingSegmentsQueried=" + _numConsumingSegmentsQueried
                + ", _numConsumingSegmentsProcessed=" + _numConsumingSegmentsProcessed
                + ", _numConsumingSegmentsMatched=" + _numConsumingSegmentsMatched + ", _minConsumingFreshnessTimeMs="
                + _minConsumingFreshnessTimeMs + ", _totalDocs=" + _totalDocs + ", _numGroupsLimitReached="
                + _numGroupsLimitReached + ", _timeUsedMs=" + _timeUsedMs + ", _offlineThreadCpuTimeNs="
                + _offlineThreadCpuTimeNs + ", _realtimeThreadCpuTimeNs=" + _realtimeThreadCpuTimeNs
                + ", _offlineSystemActivitiesCpuTimeNs=" + _offlineSystemActivitiesCpuTimeNs
                + ", _realtimeSystemActivitiesCpuTimeNs=" + _realtimeSystemActivitiesCpuTimeNs
                + ", _offlineResponseSerializationCpuTimeNs=" + _offlineResponseSerializationCpuTimeNs
                + ", _realtimeResponseSerializationCpuTimeNs=" + _realtimeResponseSerializationCpuTimeNs
                + ", _offlineTotalCpuTimeNs=" + _offlineTotalCpuTimeNs + ", _realtimeTotalCpuTimeNs="
                + _realtimeTotalCpuTimeNs + ", _numSegmentsPrunedByBroker=" + _numSegmentsPrunedByBroker
                + ", _numSegmentsPrunedByServer=" + _numSegmentsPrunedByServer + ", _numSegmentsPrunedInvalid="
                + _numSegmentsPrunedInvalid + ", _numSegmentsPrunedByLimit=" + _numSegmentsPrunedByLimit
                + ", _numSegmentsPrunedByValue=" + _numSegmentsPrunedByValue + ", _explainPlanNumEmptyFilterSegments="
                + _explainPlanNumEmptyFilterSegments + ", _explainPlanNumMatchAllFilterSegments="
                + _explainPlanNumMatchAllFilterSegments + ", _numRowsResultSet=" + _numRowsResultSet + ", _tableNames="
                + _tableNames + ", _offlineServerTenant='" + _offlineServerTenant + '\'' + ", _realtimeServerTenant='"
                + _realtimeServerTenant + '\'' + '}';
    }
}
