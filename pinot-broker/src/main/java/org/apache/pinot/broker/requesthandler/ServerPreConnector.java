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
package org.apache.pinot.broker.requesthandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Opens broker-to-server Netty channels ahead of query traffic, so the first real query does not pay
/// the blocking `connect()` -- and, when broker-to-server TLS is on, the handshake -- on its critical
/// path.
///
/// `ServerRoutingInstance` identity includes the table type, so OFFLINE and REALTIME are **separate**
/// channels to the same physical server; both are connected here. Connecting an already-active channel
/// is a no-op, so this is safe to call more than once.
///
/// Bounded on two axes so it can never stall startup: a capped thread pool, and a per-channel wait
/// clamped to the caller's deadline. A server that is unreachable or itself restarting is logged and
/// skipped -- the existing lazy-connect path still serves it. This class is stateless and thread-safe.
///
/// It takes its dependencies as functions rather than concrete `RoutingManager`/`QueryRouter` types so
/// the parallelism, budget and failure handling can be unit-tested without a live broker.
@ThreadSafe
public class ServerPreConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPreConnector.class);

  /// Cap on the connect thread pool: a large tenant must not spawn a thread per server. Safe to exceed
  /// the core count even on a 2- or 4-vCPU broker: each task is blocking connect + TLS handshake (mostly
  /// network wait, with the actual I/O on Netty's event loop), and this runs during startup before any
  /// query load, so the threads are almost entirely parked rather than contending for CPU.
  @VisibleForTesting
  static final int MAX_CONNECT_THREADS = 16;

  private final Supplier<Collection<ServerInstance>> _routableServersSupplier;
  private final BiPredicate<ServerInstance, TableType> _connectFn;

  /// @param routableServersSupplier supplies the servers to connect, evaluated once per [#preConnect]
  ///     call after the caller has ensured routing is built
  /// @param connectFn opens the channel for one (server, table type) and returns whether it succeeded
  public ServerPreConnector(Supplier<Collection<ServerInstance>> routableServersSupplier,
      BiPredicate<ServerInstance, TableType> connectFn) {
    _routableServersSupplier = routableServersSupplier;
    _connectFn = connectFn;
  }

  /// Opens a channel to every routable server, for both table types, in parallel, bounded by
  /// `deadlineMs` (an absolute [System#currentTimeMillis] value). Returns the number of channels
  /// successfully connected. Never throws: a channel that fails or times out is logged and skipped.
  public int preConnect(long deadlineMs) {
    // Snapshot the routable-server view once. The supplier may return a live map view that another thread
    // updates during startup; snapshotting keeps the channel count consistent with the tasks actually
    // submitted below, so we never poll for phantom channels or under-count real ones.
    List<ServerInstance> servers = new ArrayList<>(_routableServersSupplier.get());
    if (servers.isEmpty() || System.currentTimeMillis() >= deadlineMs) {
      return 0;
    }
    long startMs = System.currentTimeMillis();
    int channelCount = servers.size() * TableType.values().length;
    ExecutorService executor = Executors.newFixedThreadPool(Math.min(channelCount, MAX_CONNECT_THREADS),
        new ThreadFactoryBuilder().setNameFormat("broker-preconnect-%d").setDaemon(true).build());
    // A completion service hands channels back in the order they finish, not the order submitted, so a
    // slow or unreachable server never blocks the counting of faster ones ahead of the shared deadline
    // -- no head-of-line blocking, and no under-count of channels that already connected in parallel.
    CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
    int connected = 0;
    try {
      for (ServerInstance server : servers) {
        for (TableType tableType : TableType.values()) {
          completionService.submit(() -> _connectFn.test(server, tableType));
        }
      }
      for (int i = 0; i < channelCount; i++) {
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          break;
        }
        try {
          Future<Boolean> future = completionService.poll(remainingMs, TimeUnit.MILLISECONDS);
          if (future == null) {
            // Budget elapsed before the next channel finished; the rest fall back to the lazy path.
            break;
          }
          if (Boolean.TRUE.equals(future.get())) {
            connected++;
          }
        } catch (InterruptedException e) {
          // Shutdown: stopPreConnect() interrupts us. Restore the flag and stop promptly.
          Thread.currentThread().interrupt();
          break;
        } catch (ExecutionException e) {
          // A server that is unreachable or itself restarting must not block startup.
          LOGGER.debug("Pre-connect did not complete for one channel", e);
        }
      }
    } finally {
      executor.shutdownNow();
    }
    LOGGER.info("Broker pre-connected {}/{} channel(s) across {} server(s) in {} ms", connected,
        channelCount, servers.size(), System.currentTimeMillis() - startMs);
    return connected;
  }
}
