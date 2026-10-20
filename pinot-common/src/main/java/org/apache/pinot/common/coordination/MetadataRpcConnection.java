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
package org.apache.pinot.common.coordination;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;

import static com.google.common.base.Preconditions.checkArgument;


/// Bounded endpoint failover that resends the identical application request after an unknown transport outcome.
/// Instances own their channels; membership uses a separate instance from metadata hydration and installation.
public class MetadataRpcConnection implements AutoCloseable {
  private final List<MetadataRpcClient> _clients;
  private final AtomicInteger _preferred = new AtomicInteger();

  /// A TLS endpoint whose hostname is verified against the peer certificate.
  public static final class Endpoint {
    private final String _host;
    private final int _port;

    public Endpoint(String host, int port) {
      checkArgument(host != null && !host.isBlank() && port > 0 && port <= 65535, "Invalid metadata endpoint");
      _host = host;
      _port = port;
    }

    public String host() {
      return _host;
    }

    public int port() {
      return _port;
    }
  }

  public MetadataRpcConnection(List<Endpoint> endpoints, Path certificate, Path key, Path trust,
      long timeoutMillis) throws IOException {
    checkArgument(endpoints != null && !endpoints.isEmpty() && endpoints.size() <= 16,
        "Between one and sixteen metadata endpoints are required");
    List<MetadataRpcClient> clients = new ArrayList<>();
    try {
      for (Endpoint endpoint : List.copyOf(endpoints)) {
        clients.add(new MetadataRpcClient(endpoint.host(), endpoint.port(), certificate, key, trust, timeoutMillis));
      }
    } catch (IOException | RuntimeException e) {
      clients.forEach(MetadataRpcClient::close);
      throw e;
    }
    _clients = List.copyOf(clients);
  }

  public MetadataRpc.MembershipResponse membershipRead(MetadataRpc.MembershipReadRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.membershipRead(request, timeout));
  }

  public MetadataRpc.RoutingSnapshot routingSnapshot(MetadataRpc.RoutingSnapshotRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.routingSnapshot(request, timeout));
  }

  public MetadataRpc.MembershipResponse membershipExecute(MetadataRpc.MembershipCommandRequest request,
      long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.membershipExecute(request, timeout));
  }

  public MetadataRpc.CommandResult taskExecute(MetadataRpc.TaskCommandRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.taskExecute(request, timeout));
  }

  public MetadataRpc.TaskWorkResult taskWork(MetadataRpc.TaskWorkRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.taskWork(request, timeout));
  }

  public MetadataRpc.CommandResult taskOutcome(MetadataRpc.TaskOutcomeRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.taskOutcome(request, timeout));
  }

  public MetadataRpc.InstallationSnapshot snapshot(MetadataRpc.InstallationSnapshotRequest request,
      long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.installationSnapshot(request, timeout));
  }

  public MetadataRpc.InstallationWorkResult lookup(MetadataRpc.InstallationLookupRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.installationLookup(request, timeout));
  }

  public MetadataRpc.CommandResult execute(MetadataRpc.InstallationCommandRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.installationExecute(request, timeout));
  }

  public MetadataRpc.CommandResult outcome(MetadataRpc.InstallationOutcomeRequest request, long timeoutMillis) {
    return call(timeoutMillis, (client, timeout) -> client.installationOutcome(request, timeout));
  }

  private <T> T call(long timeoutMillis, BiFunction<MetadataRpcClient, Long, T> operation) {
    checkArgument(timeoutMillis > 0 && timeoutMillis <= 30_000, "Metadata deadline must be between 1 and 30000 ms");
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    int first = _preferred.get();
    StatusRuntimeException failure = null;
    for (int attempt = 0; attempt < _clients.size(); attempt++) {
      long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
      if (remaining <= 0) {
        break;
      }
      int selected = (first + attempt) % _clients.size();
      try {
        // Reserve time for the other configured endpoints instead of spending the whole deadline on a dead peer.
        T result = operation.apply(_clients.get(selected), Math.max(1, remaining / (_clients.size() - attempt)));
        _preferred.set(selected);
        return result;
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.UNAVAILABLE
            && e.getStatus().getCode() != Status.Code.DEADLINE_EXCEEDED) {
          throw e;
        }
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
    throw Status.DEADLINE_EXCEEDED.withDescription("Metadata endpoint deadline expired").asRuntimeException();
  }

  @Override
  public void close() {
    _clients.forEach(MetadataRpcClient::close);
  }
}
