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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.cursors.AbstractResultStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


public class CursorRequestHandlerDelegate {
  private final BrokerRequestHandler _brokerRequestHandler;
  private final AbstractResultStore _resultStore;
  private final String _brokerHost;
  private final int _brokerPort;
  private final long _expirationIntervalInMs;

  public CursorRequestHandlerDelegate(String brokerHost, int brokerPort, AbstractResultStore resultStore,
      BrokerRequestHandler brokerRequestHandler, String expirationTime) {
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _resultStore = resultStore;
    _brokerRequestHandler = brokerRequestHandler;
    _expirationIntervalInMs = TimeUtils.convertPeriodToMillis(expirationTime);
  }

  public CursorResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders,
      int numRows)
      throws Exception {
    BrokerResponse response =
        _brokerRequestHandler.handleRequest(request, sqlNodeAndOptions, requesterIdentity, requestContext, httpHeaders);

    if (response.getExceptionsSize() > 0) {
      CursorResponseNative cursorResponseNative = new CursorResponseNative();
      cursorResponseNative.setBrokerId(response.getBrokerId());
      cursorResponseNative.setExceptions(response.getExceptions());
      return cursorResponseNative;
    }

    CursorResponse cursorResponse = createCursorResponse(response);

    long submissionTimeMs = System.currentTimeMillis();
    // Initialize all CursorResponse specific metadata
    cursorResponse.setBrokerHost(_brokerHost);
    cursorResponse.setBrokerPort(_brokerPort);
    cursorResponse.setSubmissionTimeMs(submissionTimeMs);
    cursorResponse.setExpirationTimeMs(submissionTimeMs + _expirationIntervalInMs);
    cursorResponse.setOffset(0);
    cursorResponse.setNumRows(response.getNumRowsResultSet());

    long cursorStoreStartTimeMs = System.currentTimeMillis();
    _resultStore.writeResponse(cursorResponse);
    long cursorStoreTimeMs = System.currentTimeMillis() - cursorStoreStartTimeMs;
    CursorResponse cursorResponse1 = _resultStore.handleCursorRequest(response.getRequestId(), 0, numRows);
    cursorResponse1.setCursorResultWriteTimeMs(cursorStoreTimeMs);
    return cursorResponse1;
  }

  public static CursorResponseNative createCursorResponse(BrokerResponse response) {
    CursorResponseNative responseNative = new CursorResponseNative();

    // Copy all the member variables of BrokerResponse to CursorResponse.
    responseNative.setResultTable(response.getResultTable());
    responseNative.setNumRowsResultSet(response.getNumRowsResultSet());
    responseNative.setExceptions(response.getExceptions());
    responseNative.setNumGroupsLimitReached(response.isNumGroupsLimitReached());
    responseNative.setTimeUsedMs(response.getTimeUsedMs());
    responseNative.setRequestId(response.getRequestId());
    responseNative.setBrokerId(response.getBrokerId());
    responseNative.setNumDocsScanned(response.getNumDocsScanned());
    responseNative.setTotalDocs(response.getTotalDocs());
    responseNative.setNumEntriesScannedInFilter(response.getNumEntriesScannedInFilter());
    responseNative.setNumEntriesScannedPostFilter(response.getNumEntriesScannedPostFilter());
    responseNative.setNumServersQueried(response.getNumServersQueried());
    responseNative.setNumServersResponded(response.getNumServersResponded());
    responseNative.setNumSegmentsQueried(response.getNumSegmentsQueried());
    responseNative.setNumSegmentsProcessed(response.getNumSegmentsProcessed());
    responseNative.setNumSegmentsMatched(response.getNumSegmentsMatched());
    responseNative.setNumConsumingSegmentsQueried(response.getNumConsumingSegmentsQueried());
    responseNative.setNumConsumingSegmentsProcessed(response.getNumConsumingSegmentsProcessed());
    responseNative.setNumConsumingSegmentsMatched(response.getNumConsumingSegmentsMatched());
    responseNative.setMinConsumingFreshnessTimeMs(response.getMinConsumingFreshnessTimeMs());
    responseNative.setNumSegmentsPrunedByBroker(response.getNumSegmentsPrunedByBroker());
    responseNative.setNumSegmentsPrunedByServer(response.getNumSegmentsPrunedByServer());
    responseNative.setNumSegmentsPrunedInvalid(response.getNumSegmentsPrunedInvalid());
    responseNative.setNumSegmentsPrunedByLimit(response.getNumSegmentsPrunedByLimit());
    responseNative.setNumSegmentsPrunedByValue(response.getNumSegmentsPrunedByValue());
    responseNative.setBrokerReduceTimeMs(response.getBrokerReduceTimeMs());
    responseNative.setOfflineThreadCpuTimeNs(response.getOfflineThreadCpuTimeNs());
    responseNative.setRealtimeThreadCpuTimeNs(response.getRealtimeThreadCpuTimeNs());
    responseNative.setOfflineSystemActivitiesCpuTimeNs(response.getOfflineSystemActivitiesCpuTimeNs());
    responseNative.setRealtimeSystemActivitiesCpuTimeNs(response.getRealtimeSystemActivitiesCpuTimeNs());
    responseNative.setOfflineResponseSerializationCpuTimeNs(response.getOfflineResponseSerializationCpuTimeNs());
    responseNative.setRealtimeResponseSerializationCpuTimeNs(response.getRealtimeResponseSerializationCpuTimeNs());
    responseNative.setExplainPlanNumEmptyFilterSegments(response.getExplainPlanNumEmptyFilterSegments());
    responseNative.setExplainPlanNumMatchAllFilterSegments(response.getExplainPlanNumMatchAllFilterSegments());
    responseNative.setTraceInfo(response.getTraceInfo());

    return responseNative;
  }
}
