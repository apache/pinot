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
package org.apache.pinot.java11;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.pinot.client.BrokerResponse;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.PinotClientTransport;


/// A [PinotClientTransport] that replays a fixed broker response instead of talking to a broker, so the verifier
/// can drive the real `Connection` / `ResultSetGroup` / JDBC code paths without standing up a cluster.
///
/// Records the last query it was handed so callers can assert that query construction
/// (for example `PreparedStatement` parameter binding) happened on the way in.
///
/// Not thread-safe: the verifier drives it from a single thread.
final class CannedResponseTransport implements PinotClientTransport<Void> {
  private final JsonNode _cannedResponse;
  private String _lastQuery;
  private boolean _closed;

  CannedResponseTransport(JsonNode cannedResponse) {
    _cannedResponse = cannedResponse;
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException {
    _lastQuery = query;
    return BrokerResponse.fromJson(_cannedResponse);
  }

  @Override
  public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
      throws PinotClientException {
    return CompletableFuture.completedFuture(executeQuery(brokerAddress, query));
  }

  @Override
  public void close()
      throws PinotClientException {
    _closed = true;
  }

  /// The last query handed to this transport, or null if it has not been asked to execute one yet.
  @Nullable
  String getLastQuery() {
    return _lastQuery;
  }

  boolean isClosed() {
    return _closed;
  }
}
