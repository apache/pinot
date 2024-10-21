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
import org.apache.pinot.broker.requesthandler.BrokerRequestHandlerDelegate;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.cursors.ResponseSerde;
import org.apache.pinot.spi.cursors.ResponseStore;
import org.apache.pinot.spi.env.PinotConfiguration;


@AutoService(ResponseStore.class)
public class MemoryResultStore extends AbstractResponseStore {
  private final Map<String, CursorResponse> _cursorResponseMap = new HashMap<>();
  private final Map<String, ResultTable> _resultTableMap = new HashMap<>();

  private static final String TYPE = "memory";

  private BrokerMetrics _brokerMetrics;

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  protected void writeResponse(String requestId, CursorResponse response)
      throws Exception {
    _cursorResponseMap.put(requestId, response);
  }

  @Override
  protected long writeResultTable(String requestId, ResultTable resultTable)
      throws Exception {
    _resultTableMap.put(requestId, resultTable);
    return 0;
  }

  @Override
  public CursorResponse readResponse(String requestId)
      throws Exception {
    CursorResponse response = _cursorResponseMap.get(requestId);
    CursorResponse responseCopy = BrokerRequestHandlerDelegate.createCursorResponse(response);

    responseCopy.setBrokerHost(response.getBrokerHost());
    responseCopy.setBrokerPort(response.getBrokerPort());
    responseCopy.setSubmissionTimeMs(response.getSubmissionTimeMs());
    responseCopy.setExpirationTimeMs(response.getExpirationTimeMs());
    return responseCopy;
  }

  @Override
  protected ResultTable readResultTable(String requestId)
      throws Exception {
      return _resultTableMap.get(requestId);
  }

  @Override
  public void init(PinotConfiguration config, BrokerMetrics brokerMetrics, ResponseSerde responseSerde)
      throws Exception {
    _brokerMetrics = brokerMetrics;
  }

  @Override
  protected BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  @Override
  public boolean exists(String requestId)
      throws Exception {
    return _cursorResponseMap.containsKey(requestId) && _resultTableMap.containsKey(requestId);
  }

  @Override
  public Collection<String> getAllStoredRequestIds()
      throws Exception {
    return _cursorResponseMap.keySet();
  }

  @Override
  protected boolean deleteResponseImpl(String requestId)
      throws Exception {
    return _cursorResponseMap.remove(requestId) != null && _resultTableMap.remove(requestId) != null;
  }
}
