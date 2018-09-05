/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.transport;

import com.linkedin.pinot.common.utils.DataTable;
import javax.annotation.concurrent.ThreadSafe;


/**
 * The {@code ServerResponse} class contains the response and time info from a {@link Server}.
 */
@ThreadSafe
public class ServerResponse {
  private final long _startTimeMs;
  private volatile long _submitRequestTimeMs;
  private volatile long _receiveDataTabTimeMs;
  private volatile DataTable _dataTable;
  private volatile long _responseSize;
  private volatile long _deserializationTimeMs;

  public ServerResponse(long startTimeMs) {
    _startTimeMs = startTimeMs;
  }

  public DataTable getDataTable() {
    return _dataTable;
  }

  public long getSubmitDelayMs() {
    if (_submitRequestTimeMs != 0) {
      return _submitRequestTimeMs - _startTimeMs;
    } else {
      return -1;
    }
  }

  public long getResponseDelayMs() {
    if (_receiveDataTabTimeMs != 0) {
      return _receiveDataTabTimeMs - _submitRequestTimeMs;
    } else {
      return -1;
    }
  }

  public long getResponseSize() {
    return _responseSize;
  }

  public long getDeserializationTimeMs() {
    return _deserializationTimeMs;
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%d,%d", getSubmitDelayMs(), getResponseDelayMs(), getResponseSize(),
        getDeserializationTimeMs());
  }

  void markRequestSubmitted() {
    _submitRequestTimeMs = System.currentTimeMillis();
  }

  void receiveDataTable(DataTable dataTable, long responseSize, long deserializationTimeMs) {
    _receiveDataTabTimeMs = System.currentTimeMillis();
    _dataTable = dataTable;
    _responseSize = responseSize;
    _deserializationTimeMs = deserializationTimeMs;
  }
}
