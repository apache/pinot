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
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.TimeUtils;


public abstract class AbstractResponseStore implements ResponseStore {

  protected String _brokerHost;
  protected int _brokerPort;
  protected String _brokerId;
  protected BrokerMetrics _brokerMetrics;
  protected long _expirationIntervalInMs;

  protected void init(String brokerHost, int brokerPort, String brokerId, BrokerMetrics brokerMetrics,
      String expirationTime) {
    _brokerMetrics = brokerMetrics;
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _brokerId = brokerId;
    _expirationIntervalInMs = TimeUtils.convertPeriodToMillis(expirationTime);
  }

  /**
   * Initialize the store.
   * @param config Subset configuration of pinot.broker.cursor.response.store.&lt;type&gt;
   * @param brokerHost Hostname of the broker where ResponseStore is created
   * @param brokerPort Port of the broker where the ResponseStore is created
   * @param brokerId ID of the broker where the ResponseStore is created.
   * @param brokerMetrics Metrics utility to track cursor metrics.
   */
  public abstract void init(PinotConfiguration config, String brokerHost, int brokerPort, String brokerId,
      BrokerMetrics brokerMetrics, String expirationTime)
      throws Exception;

  /**
   * Get the hostname of the broker where the query is executed
   * @return String containing the hostname
   */
  protected String getBrokerHost() {
    return _brokerHost;
  }

  /**
   * Get the port of the broker where the query is executed
   * @return int containing the port
   */
  protected int getBrokerPort() {
    return _brokerPort;
  }

  /**
   * Get the expiration interval of a query response.
   * @return long containing the expiration interval.
   */
  protected long getExpirationIntervalInMs() {
    return _expirationIntervalInMs;
  }

  /**
   * Write a CursorResponse
   * @param requestId Request ID of the response
   * @param response The response to write
   * @throws Exception Thrown if there is any error while writing the response
   */
  protected abstract void writeResponse(String requestId, CursorResponse response)
      throws Exception;

  /**
   * Write a {@link ResultTableRows} to the store
   * @param requestId Request ID of the response
   * @param resultTableRows The {@link ResultTableRows} of the query
   * @throws Exception Thrown if there is any error while writing the result table.
   * @return Returns the number of bytes written
   */
  protected abstract long writeResultTable(String requestId, ResultTableRows resultTableRows)
      throws Exception;

  /**
   * Read the response (excluding the {@link ResultTableRows}) from the store
   * @param requestId Request ID of the response
   * @return CursorResponse (without the {@link ResultTableRows})
   * @throws Exception Thrown if there is any error while reading the response
   */
  public abstract CursorResponse readResponse(String requestId)
      throws Exception;

  /**
   * Read the {@link ResultTableRows} of a query response
   * @param requestId Request ID of the query
   * @param offset Offset of the result slice
   * @param numRows Number of rows required in the slice
   * @return {@link ResultTableRows} of the query
   * @throws Exception Thrown if there is any error while reading the result table
   */
  protected abstract ResultTableRows readResultTable(String requestId, int offset, int numRows)
      throws Exception;

  protected abstract boolean deleteResponseImpl(String requestId)
      throws Exception;

  /**
   * Stores the response in the store. {@link CursorResponse} and {@link ResultTableRows} are stored separately.
   * @param response Response to be stored
   * @throws Exception Thrown if there is any error while storing the response.
   */
  public void storeResponse(BrokerResponse response)
      throws Exception {
    String requestId = response.getRequestId();

    CursorResponse cursorResponse = new CursorResponseNative(response);

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
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.CURSOR_RESPONSE_STORE_SIZE, bytesWritten);
    } catch (Exception e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.CURSOR_WRITE_EXCEPTION, 1);
      deleteResponse(requestId);
      throw e;
    }
  }

  /**
   * Reads the response from the store and populates it with a slice of the {@link ResultTableRows}
   * @param requestId Request ID of the query
   * @param offset Offset of the result slice
   * @param numRows Number of rows required in the slice
   * @return A CursorResponse with a slice of the {@link ResultTableRows}
   * @throws Exception Thrown if there is any error during the operation.
   */
  public CursorResponse handleCursorRequest(String requestId, int offset, int numRows)
      throws Exception {

    CursorResponse response;
    ResultTableRows resultTableRows;

    try {
      response = readResponse(requestId);
    } catch (Exception e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.CURSOR_READ_EXCEPTION, 1);
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
      resultTableRows = readResultTable(requestId, offset, numRows);
    } catch (Exception e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.CURSOR_READ_EXCEPTION, 1);
      throw e;
    }

    response.setResultTable(resultTableRows);
    response.setCursorFetchTimeMs(System.currentTimeMillis() - fetchStartTime);
    response.setOffset(offset);
    response.setNumRows(resultTableRows.getRows().size());
    response.setNumRowsResultSet(totalTableRows);
    return response;
  }

  /**
   * Returns the list of responses created by the broker.
   * Note that the ResponseStore object in a broker should only return responses created by it.
   * @return A list of CursorResponse objects created by the specific broker
   * @throws Exception Thrown if there is an error during an operation.
   */
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
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.CURSOR_RESPONSE_STORE_SIZE, bytesWritten * -1);
    }
    return isSucceeded;
  }
}
