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

import java.util.concurrent.CompletableFuture;


public class DummyPinotClientTransport implements PinotClientTransport {
  private String _lastQuery;

  @Override
  public BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException {
    _lastQuery = query;
    return BrokerResponse.empty();
  }

  @Override
  public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
      throws PinotClientException {
    _lastQuery = query;
    return null;
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, Request request)
      throws PinotClientException {
    _lastQuery = request.getQuery();
    return BrokerResponse.empty();
  }

  @Override
  public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
      throws PinotClientException {
    _lastQuery = request.getQuery();
    return null;
  }

  public String getLastQuery() {
    return _lastQuery;
  }

  @Override
  public void close()
      throws PinotClientException {
  }
}
