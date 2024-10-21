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
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseSerde;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;


public abstract class AbstractResponseStore implements ResponseStore {

  /**
   * Initialize the store.
   * @param config Configuration of the store.
   * @param brokerMetrics Metrics utility to track cursor metrics.
   * @param responseSerde The Serde object to use to serialize/deserialize the responses
   */
  public abstract void init(PinotConfiguration config, BrokerMetrics brokerMetrics, ResponseSerde responseSerde)
      throws Exception;

  protected abstract BrokerMetrics getBrokerMetrics();

  /**
   * Write a CursorResponse
   * @param requestId Request ID of the response
   * @param response The response to write
   * @throws Exception Thrown if there is any error while writing the response
   */
  protected abstract void writeResponse(String requestId, CursorResponse response)
      throws Exception;

  /**
   * Write a @link{ResultTable} to the store
   * @param requestId Request ID of the response
   * @param resultTable The @link{ResultTable} of the query
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
   * @return @link{ResultTable} of the query
   * @throws Exception Thrown if there is any error while reading the result table
   */
  protected abstract ResultTable readResultTable(String requestId)
      throws Exception;

  protected abstract boolean deleteResponseImpl(String requestId)
      throws Exception;

  /**
   * Stores the response in the store. @link{CursorResponse} and @link{ResultTable} are stored separately.
   * @param response Response to be stored
   * @throws Exception Thrown if there is any error while storing the response.
   */
  public void storeResponse(CursorResponse response)
      throws Exception {
    String requestId = response.getRequestId();

    try {
      long bytesWritten = writeResultTable(requestId, response.getResultTable());

      // Remove the resultTable from the response as it is serialized in a data file.
      response.setResultTable(null);
      response.setBytesWritten(bytesWritten);
      writeResponse(requestId, response);
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_RESULT_STORE_SIZE, bytesWritten);
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
      throw new RuntimeException("Offset " + offset + " is greater than totalRecords " + totalTableRows);
    }

    long fetchStartTime = System.currentTimeMillis();
    try {
      resultTable = readResultTable(requestId);
    } catch (Exception e) {
      getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_READ_EXCEPTION, 1);
      throw e;
    }

    int sliceEnd = offset + numRows;
    if (sliceEnd > totalTableRows) {
      sliceEnd = totalTableRows;
      numRows = sliceEnd - offset;
    }

    response.setResultTable(
        new ResultTable(resultTable.getDataSchema(), resultTable.getRows().subList(offset, sliceEnd)));
    response.setCursorFetchTimeMs(System.currentTimeMillis() - fetchStartTime);
    response.setOffset(offset);
    response.setNumRows(numRows);
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
    getBrokerMetrics().addMeteredGlobalValue(BrokerMeter.CURSOR_RESULT_STORE_SIZE, bytesWritten * -1);
    return deleteResponseImpl(requestId);
  }
}
