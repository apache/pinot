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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.pinot.common.response.CursorResponse;


@JsonPropertyOrder({
    "resultTable", "numRowsResultSet", "partialResult", "exceptions", "numGroupsLimitReached", "timeUsedMs",
    "requestId", "brokerId", "numDocsScanned", "totalDocs", "numEntriesScannedInFilter", "numEntriesScannedPostFilter",
    "numServersQueried", "numServersResponded", "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched",
    "numConsumingSegmentsQueried", "numConsumingSegmentsProcessed", "numConsumingSegmentsMatched",
    "minConsumingFreshnessTimeMs", "numSegmentsPrunedByBroker", "numSegmentsPrunedByServer",
    "numSegmentsPrunedInvalid", "numSegmentsPrunedByLimit", "numSegmentsPrunedByValue", "brokerReduceTimeMs",
    "offlineThreadCpuTimeNs", "realtimeThreadCpuTimeNs", "offlineSystemActivitiesCpuTimeNs",
    "realtimeSystemActivitiesCpuTimeNs", "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs", "offlineTotalCpuTimeNs", "realtimeTotalCpuTimeNs",
    "explainPlanNumEmptyFilterSegments", "explainPlanNumMatchAllFilterSegments", "traceInfo",
    // Fields specific to CursorResponse
    "offset", "numRows", "cursorResultWriteTimeMs", "cursorResultWriteTimeMs", "submissionTimeMs", "expirationTimeMs",
    "brokerHost", "brokerPort", "bytesWritten"
})
public class CursorResponseNative extends BrokerResponseNative implements CursorResponse {
  private int _offset;
  private int _numRows;
  private long _cursorResultWriteTimeMs;
  private long _cursorFetchTimeMs;
  private long _submissionTimeMs;
  private long _expirationTimeMs;
  private String _brokerHost;
  private int _brokerPort;
  private long _bytesWritten;

  public CursorResponseNative() {
  }

  @Override
  public String getBrokerHost() {
    return _brokerHost;
  }

  @Override
  public void setBrokerHost(String brokerHost) {
    _brokerHost = brokerHost;
  }

  @Override
  public int getBrokerPort() {
    return _brokerPort;
  }

  @Override
  public void setBrokerPort(int brokerPort) {
    _brokerPort = brokerPort;
  }

  @Override
  public void setOffset(int offset) {
    _offset = offset;
  }

  @Override
  public void setNumRows(int numRows) {
    _numRows = numRows;
  }

  @Override
  public void setCursorFetchTimeMs(long cursorFetchTimeMs) {
    _cursorFetchTimeMs = cursorFetchTimeMs;
  }

  public long getSubmissionTimeMs() {
    return _submissionTimeMs;
  }

  @Override
  public void setSubmissionTimeMs(long submissionTimeMs) {
    _submissionTimeMs = submissionTimeMs;
  }

  public long getExpirationTimeMs() {
    return _expirationTimeMs;
  }

  @Override
  public void setBytesWritten(long bytesWritten) {
    _bytesWritten = bytesWritten;
  }

  @Override
  public long getBytesWritten() {
    return _bytesWritten;
  }

  @Override
  public void setExpirationTimeMs(long expirationTimeMs) {
    _expirationTimeMs = expirationTimeMs;
  }

  @Override
  public int getOffset() {
    return _offset;
  }

  @Override
  public int getNumRows() {
    return _numRows;
  }

  @JsonProperty("cursorResultWriteTimeMs")
  public long getCursorResultWriteTimeMs() {
    return _cursorResultWriteTimeMs;
  }

  @Override
  public void setCursorResultWriteTimeMs(long cursorResultWriteMs) {
    _cursorResultWriteTimeMs = cursorResultWriteMs;
  }

  public long getCursorFetchTimeMs() {
    return _cursorFetchTimeMs;
  }
}
