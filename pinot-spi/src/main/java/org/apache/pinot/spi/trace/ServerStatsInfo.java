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
package org.apache.pinot.spi.trace;

public class ServerStatsInfo {
    private final long _submitRequestTimeMs;
    private final int _requestSentLatencyMs;
    private final long _receiveDataTableTimeMs;
    private final int _responseSize;
    private final int _deserializationTimeMs;

    public ServerStatsInfo(long submitRequestTimeMs, int requestSentLatencyMs,
        long receiveDataTableTimeMs, int responseSize, int deserializationTimeMs) {
      _submitRequestTimeMs = submitRequestTimeMs;
      _requestSentLatencyMs = requestSentLatencyMs;
      _receiveDataTableTimeMs = receiveDataTableTimeMs;
      _responseSize = responseSize;
      _deserializationTimeMs = deserializationTimeMs;
    }

    public long getSubmitRequestTimeMs() {
      return _submitRequestTimeMs;
    }

    public int getRequestSentLatencyMs() {
      return _requestSentLatencyMs;
    }

    public long getReceiveDataTableTimeMs() {
      return _receiveDataTableTimeMs;
    }

    public int getResponseSize() {
      return _responseSize;
    }

    public int getDeserializationTimeMs() {
      return _deserializationTimeMs;
    }
}
