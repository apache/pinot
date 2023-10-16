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


/**
 * Interface for plugging different client transports.
 */
public interface PinotClientTransport<METRICS> {

  BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException;

  CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
      throws PinotClientException;

  void close()
      throws PinotClientException;

  /**
   * Access to the client metrics implementation if any.
   * This may be useful for observability into the client implementation.
   *
   * @return underlying client metrics if any
   */
  default METRICS getClientMetrics() {
    throw new UnsupportedOperationException("No useful client metrics available");
  }
}
