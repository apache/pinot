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
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResultStore;


public abstract class AbstractResultStore implements ResultStore {

  public abstract void writeResponse(CursorResponse response)
      throws Exception;

  public abstract CursorResponse readResponse(String requestId)
      throws Exception;

  protected abstract ResultTable readResultTable(String requestId)
      throws Exception;

  public CursorResponse handleCursorRequest(String requestId, int offset, int numRows)
      throws Exception {

    CursorResponse response = readResponse(requestId);
    int totalTableRows = response.getNumRowsResultSet();
    if (offset < totalTableRows) {
      long fetchStartTime = System.currentTimeMillis();
      ResultTable resultTable = readResultTable(requestId);

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
    } else if (totalTableRows == 0 && offset == 0) {
      // If sum records is 0, then result set is empty.
      response.setResultTable(null);
      response.setOffset(0);
      response.setNumRows(0);
      return response;
    }

    throw new RuntimeException("Offset " + offset + " is greater than totalRecords " + totalTableRows);
  }

  public List<CursorResponse> getAllStoredResponses()
      throws Exception {
    List<CursorResponse> responses = new ArrayList<>();

    for (String requestId : getAllStoredRequestIds()) {
      responses.add(readResponse(requestId));
    }

    return responses;
  }
}
