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
package org.apache.pinot.common.cursors;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;


public abstract class AbstractResponseStore implements ResponseStore {

  /**
   * Initialize the store.
   * @param config Subset configuration of pinot.broker.cursor.response.store.&lt;type&gt;
   * @param brokerHost Hostname of the broker where ResponseStore is created
   * @param brokerPort Port of the broker where the ResponseStore is created
   * @param brokerMetrics Metrics utility to track cursor metrics.
   */
  public abstract void init(PinotConfiguration config, String brokerHost, int brokerPort, BrokerMetrics brokerMetrics,
      String expirationTime)
      throws Exception;

  /**
   * Get the BrokerMetrics object to update metrics
   * @return A BrokerMetrics object
   */
  protected abstract BrokerMetrics getBrokerMetrics();

  /**
   * Get the hostname of the broker where the query is executed
   * @return String containing the hostname
   */
  protected abstract String getBrokerHost();

  /**
   * Get the port of the broker where the query is executed
   * @return int containing the port
   */
  protected abstract int getBrokerPort();

  /**
   * Get the expiration interval of a query response.
   * @return long containing the expiration interval.
   */
  protected abstract long getExpirationIntervalInMs();

  /**
   * Write a CursorResponse
   * @param requestId Request ID of the response
   * @param response The response to write
   * @throws Exception Thrown if there is any error while writing the response
   */
  protected abstract void writeResponse(String requestId, CursorResponse response)
      throws Exception;

  /**
   * Write a {@link ResultTable} to the store
   * @param requestId Request ID of the response
   * @param resultTable The {@link ResultTable} of the query
   * @throws Exception Thrown if there is any error while writing the result table.
   * @return Returns the number of bytes written
   */
  protected abstract long writeResultTable(String requestId, ResultTable resultTable)
      throws Exception;

  /**
   * Read the response (excluding the @link{ResultTable}) from the store
   * @param requestId Request ID of the response
   * @return CursorResponse (without the @link{ResultTable})
   * @throws Exception Thrown if there is any error while reading the response
   */
  public abstract CursorResponse readResponse(String requestId)
      throws Exception;

  /**
   * Read the @link{ResultTable} of a query response
   * @param requestId Request ID of the query
   * @param offset Offset of the result slice
   * @param numRows Number of rows required in the slice
   * @return @link{ResultTable} of the query
   * @throws Exception Thrown if there is any error while reading the result table
   */
  protected abstract ResultTable readResultTable(String requestId, int offset, int numRows)
      throws Exception;

  protected abstract boolean deleteResponseImpl(String requestId)
      throws Exception;

  /**
   * Stores the response in the store. @link{CursorResponse} and @link{ResultTable} are stored separately.
   * @param response Response to be stored
   * @throws Exception Thrown if there is any error while storing the response.
   */
  public void storeResponse(BrokerResponse response)
      throws Exception {
    String requestId = response.getRequestId();

    CursorResponse cursorResponse = createCursorResponse(response);

    long submissionTimeMs = System.currentTimeMillis();
    // Initialize all CursorResponse specific metadata
    cursorResponse.setBrokerHost(getBrokerHost());
    cursorResponse.setBrokerPort(getBrokerPort());
    cursorResponse.setSubmissionTimeMs(submissionTimeMs);
    cursorResponse.setExpirationTimeMs(submissionTimeMs + getExpirationIntervalInMs());
    cursorResponse.setOffset(0);
    cursorResponse.setNumRows(response.getNumRowsResultSet());

    try {
      long bytesWritten = writeResultTable(requestId, response.getResultTable());

      // Remove the resultTable from the response as it is serialized in a data file.
      cursorResponse.setResultTable(null);
      cursorResponse.setBytesWritten(bytesWritten);
      writeResponse(requestId, cursorResponse);
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_RESPONSE_STORE_SIZE, bytesWritten);
    } catch (Exception e) {
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_WRITE_EXCEPTION, 1);
      deleteResponse(requestId);
      throw e;
    }
  }

  /**
   * Reads the response from the store and populates it with a slice of the @link{ResultTable}
   * @param requestId Request ID of the query
   * @param offset Offset of the result slice
   * @param numRows Number of rows required in the slice
   * @return A CursorResponse with a slice of the @link{ResultTable}
   * @throws Exception Thrown if there is any error during the operation.
   */
  public CursorResponse handleCursorRequest(String requestId, int offset, int numRows)
      throws Exception {

    CursorResponse response;
    ResultTable resultTable;

    try {
      response = readResponse(requestId);
    } catch (Exception e) {
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_READ_EXCEPTION, 1);
      throw e;
    }

    int totalTableRows = response.getNumRowsResultSet();

    if (totalTableRows == 0 && offset == 0) {
      // If sum records is 0, then result set is empty.
      response.setResultTable(null);
      response.setOffset(0);
      response.setNumRows(0);
      return response;
    } else if (offset >= totalTableRows) {
      throw new RuntimeException("Offset " + offset + " should be lesser than totalRecords " + totalTableRows);
    }

    long fetchStartTime = System.currentTimeMillis();
    try {
      resultTable = readResultTable(requestId, offset, numRows);
    } catch (Exception e) {
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_READ_EXCEPTION, 1);
      throw e;
    }

    response.setResultTable(resultTable);
    response.setCursorFetchTimeMs(System.currentTimeMillis() - fetchStartTime);
    response.setOffset(offset);
    response.setNumRows(resultTable.getRows().size());
    response.setNumRowsResultSet(totalTableRows);
    return response;
  }

  public List<CursorResponse> getAllStoredResponses()
      throws Exception {
    List<CursorResponse> responses = new ArrayList<>();

    for (String requestId : getAllStoredRequestIds()) {
      responses.add(readResponse(requestId));
    }

    return responses;
  }

  @Override
  public boolean deleteResponse(String requestId) throws Exception {
    if (!exists(requestId)) {
      return false;
    }

    long bytesWritten = readResponse(requestId).getBytesWritten();
    boolean isSucceeded = deleteResponseImpl(requestId);
    if (isSucceeded) {
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_RESPONSE_STORE_SIZE, bytesWritten * -1);
    }
    return isSucceeded;
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
    responseNative.setTablesQueried(response.getTablesQueried());

    return responseNative;
  }
}
