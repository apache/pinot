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
package org.apache.pinot.integration.tests.cursors;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;


@AutoService(ResponseStore.class)
public class MemoryResponseStore extends AbstractResponseStore {
  // ConcurrentHashMap required: broker cleanup cron and request-handling threads access these maps concurrently.
  private final Map<String, CursorResponse> _cursorResponseMap = new ConcurrentHashMap<>();
  private final Map<String, ResultTable> _resultTableMap = new ConcurrentHashMap<>();

  private static final String TYPE = "memory";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  protected void writeResponse(String requestId, CursorResponse response) {
    _cursorResponseMap.put(requestId, response);
  }

  @Override
  protected long writeResultTable(String requestId, ResultTable resultTable) {
    _resultTableMap.put(requestId, resultTable);
    return 0;
  }

  // throws Exception required by ResponseStore SPI contract. RuntimeExceptions guard against concurrent deletion.
  @Override
  public CursorResponse readResponse(String requestId)
      throws Exception {
    CursorResponse response = _cursorResponseMap.get(requestId);
    if (response == null) {
      throw new RuntimeException("Response not found for requestId=" + requestId);
    }
    CursorResponse responseCopy = new CursorResponseNative(response);

    responseCopy.setBrokerHost(response.getBrokerHost());
    responseCopy.setBrokerPort(response.getBrokerPort());
    responseCopy.setSubmissionTimeMs(response.getSubmissionTimeMs());
    responseCopy.setExpirationTimeMs(response.getExpirationTimeMs());
    return responseCopy;
  }

  @Override
  protected ResultTable readResultTable(String requestId, int offset, int numRows)
      throws Exception {
    CursorResponse response = _cursorResponseMap.get(requestId);
    if (response == null) {
      throw new RuntimeException("Response not found for requestId=" + requestId);
    }
    ResultTable resultTable = _resultTableMap.get(requestId);
    if (resultTable == null) {
      throw new RuntimeException("ResultTable not found for requestId=" + requestId);
    }
    int totalTableRows = response.getNumRowsResultSet();
    int sliceEnd = offset + numRows;
    if (sliceEnd > totalTableRows) {
      sliceEnd = totalTableRows;
    }

    return new ResultTable(resultTable.getDataSchema(), resultTable.getRows().subList(offset, sliceEnd));
  }

  @Override
  public void init(PinotConfiguration config, String brokerHost, int brokerPort, String brokerId,
      BrokerMetrics brokerMetrics, String expirationTime)
      throws Exception {
    init(brokerHost, brokerPort, brokerId, brokerMetrics, expirationTime);
  }

  @Override
  public boolean exists(String requestId)
      throws Exception {
    return _cursorResponseMap.containsKey(requestId) && _resultTableMap.containsKey(requestId);
  }

  @Override
  public Collection<String> getAllStoredRequestIds() {
    return new ArrayList<>(_cursorResponseMap.keySet());
  }

  @Override
  protected boolean deleteResponseImpl(String requestId) {
    return _cursorResponseMap.remove(requestId) != null & _resultTableMap.remove(requestId) != null;
  }
}
