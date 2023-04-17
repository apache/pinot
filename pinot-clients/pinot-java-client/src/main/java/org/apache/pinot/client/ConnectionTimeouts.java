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
package org.apache.pinot.client;

/**
 * Connections time out for AsyncHttpClient
 */
public class ConnectionTimeouts {
    private final int _readTimeoutMs;
    private final int _connectTimeoutMs;
    private final int _handshakeTimeoutMs;
    private final Integer _requestTimeoutMs;

    private ConnectionTimeouts(int readTimeoutMs, int connectTimeoutMs, int handshakeTimeoutMs, int requestTimeoutMs) {
        _readTimeoutMs = readTimeoutMs;
        _connectTimeoutMs = connectTimeoutMs;
        _handshakeTimeoutMs = handshakeTimeoutMs;
        _requestTimeoutMs = requestTimeoutMs;
    }

    public static ConnectionTimeouts create(int readTimeoutMs, int connectTimeoutMs, int handshakeTimeoutMs) {
        return create(readTimeoutMs, connectTimeoutMs, handshakeTimeoutMs, null);
    }

    public static ConnectionTimeouts create(int readTimeoutMs, int connectTimeoutMs, int handshakeTimeoutMs,
        Integer requestTimeoutMs) {
        if (readTimeoutMs < 1 || connectTimeoutMs < 1 || handshakeTimeoutMs < 1 || (requestTimeoutMs != null
            && requestTimeoutMs < 1)) {
            throw new IllegalArgumentException("Timeouts must be > 0");
        }
        return new ConnectionTimeouts(readTimeoutMs, connectTimeoutMs, handshakeTimeoutMs, requestTimeoutMs);
    }

    public int getReadTimeoutMs() {
        return _readTimeoutMs;
    }

    public int getConnectTimeoutMs() {
        return _connectTimeoutMs;
    }

    public int getHandshakeTimeoutMs() {
        return _handshakeTimeoutMs;
    }

    public Integer getRequestTimeoutMs() {
        return _requestTimeoutMs;
    }
}
