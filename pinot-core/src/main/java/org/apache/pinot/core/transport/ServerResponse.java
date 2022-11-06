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
package org.apache.pinot.core.transport;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;


/**
 * The {@code ServerResponse} class contains the response and time info from a {@link ServerRoutingInstance}.
 */
@ThreadSafe
public class ServerResponse {
  private final long _startTimeMs;
  private volatile long _submitRequestTimeMs;
  private volatile int _requestSentLatencyMs = -1;
  private volatile long _receiveDataTableTimeMs;
  private volatile DataTable _dataTable;
  private volatile int _responseSize;
  private volatile int _deserializationTimeMs;

  public ServerResponse(long startTimeMs) {
    _startTimeMs = startTimeMs;
  }

  @Nullable
  public DataTable getDataTable() {
    return _dataTable;
  }

  public int getSubmitDelayMs() {
    if (_submitRequestTimeMs != 0) {
      return (int) (_submitRequestTimeMs - _startTimeMs);
    } else {
      return -1;
    }
  }

  public int getRequestSentDelayMs() {
    return _requestSentLatencyMs;
  }

  public int getResponseDelayMs() {
    if (_receiveDataTableTimeMs == 0) {
      return -1;
    }
    if (_receiveDataTableTimeMs < _submitRequestTimeMs) {
      // We currently mark a request as submitted after sending the request to the server. In highQPS-lowLatency
      // usecases, there can be a race condition where the DataTable/response is received before the request is
      // marked as submitted. Return 0 to avoid reporting negative values.
      return 0;
    }

    return (int) (_receiveDataTableTimeMs - _submitRequestTimeMs);
  }

  public int getResponseSize() {
    return _responseSize;
  }

  public int getDeserializationTimeMs() {
    return _deserializationTimeMs;
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%d,%d,%d", getSubmitDelayMs(), getResponseDelayMs(), getResponseSize(),
        getDeserializationTimeMs(), getRequestSentDelayMs());
  }

  void markRequestSubmitted() {
    _submitRequestTimeMs = System.currentTimeMillis();
  }

  void markRequestSent(int requestSentLatencyMs) {
    _requestSentLatencyMs = requestSentLatencyMs;
  }

  void receiveDataTable(DataTable dataTable, int responseSize, int deserializationTimeMs) {
    _receiveDataTableTimeMs = System.currentTimeMillis();
    _dataTable = dataTable;
    _responseSize = responseSize;
    _deserializationTimeMs = deserializationTimeMs;
  }
}
