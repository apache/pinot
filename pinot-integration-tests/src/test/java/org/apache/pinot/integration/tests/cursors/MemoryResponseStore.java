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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;


@AutoService(ResponseStore.class)
public class MemoryResponseStore extends AbstractResponseStore {
  private final Map<String, CursorResponse> _cursorResponseMap = new HashMap<>();
  private final Map<String, ResultTable> _resultTableMap = new HashMap<>();

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

  @Override
  public CursorResponse readResponse(String requestId) {
    CursorResponse response = _cursorResponseMap.get(requestId);
    CursorResponse responseCopy = new CursorResponseNative(response);

    responseCopy.setBrokerHost(response.getBrokerHost());
    responseCopy.setBrokerPort(response.getBrokerPort());
    responseCopy.setSubmissionTimeMs(response.getSubmissionTimeMs());
    responseCopy.setExpirationTimeMs(response.getExpirationTimeMs());
    return responseCopy;
  }

  @Override
  protected ResultTable readResultTable(String requestId, int offset, int numRows) {
    CursorResponse response = _cursorResponseMap.get(requestId);
    int totalTableRows = response.getNumRowsResultSet();
    ResultTable resultTable = _resultTableMap.get(requestId);
    int sliceEnd = offset + numRows;
    if (sliceEnd > totalTableRows) {
      sliceEnd = totalTableRows;
    }

    return new ResultTable(resultTable.getDataSchema(), resultTable.getRows().subList(offset, sliceEnd));
  }

  @Override
  public void init(@NotNull PinotConfiguration config, @NotNull String brokerHost, int brokerPort, String brokerId,
      @NotNull BrokerMetrics brokerMetrics, String expirationTime)
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
    return _cursorResponseMap.keySet();
  }

  @Override
  protected boolean deleteResponseImpl(String requestId) {
    return _cursorResponseMap.remove(requestId) != null && _resultTableMap.remove(requestId) != null;
  }
}
