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
/// channels to the same physical server; both are connected here. Connecting only the table types a
/// server serves today would leave a table added later cold, since pre-connect is one-shot at startup.
/// Connecting an already-active channel is a no-op, so this is safe to call more than once.
///
/// Bounded on three axes so it can never stall startup: a capped thread pool, a per-channel connect
/// bound derived from the remaining budget, and a per-channel wait clamped to the caller's deadline. A
/// server that is unreachable or itself restarting is logged and skipped -- the existing lazy-connect
/// path still serves it. This class is stateless and thread-safe.
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
  ///
  /// It is a throughput cap, not a safety bound: with more channels than threads the surplus queues
  /// behind the workers, so the budget alone must not be what stops a stuck connect. That is why
  /// [ChannelConnector] takes a per-channel timeout.
  @VisibleForTesting
  static final int MAX_CONNECT_THREADS = 16;

  /// Opens one broker-to-server channel. Implementations must bound their own wait by `timeoutMs` and
  /// must not throw; the return value reports whether the channel is connected.
  @FunctionalInterface
  public interface ChannelConnector {
    boolean connect(ServerInstance serverInstance, TableType tableType, long timeoutMs);
  }

  private final Supplier<Collection<ServerInstance>> _routableServersSupplier;
  private final ChannelConnector _connector;

  /// @param routableServersSupplier supplies the servers to connect, evaluated once per [#preConnect]
  ///     call after the caller has ensured routing is built
  /// @param connector opens the channel for one (server, table type) within a timeout
  public ServerPreConnector(Supplier<Collection<ServerInstance>> routableServersSupplier,
      ChannelConnector connector) {
    _routableServersSupplier = routableServersSupplier;
    _connector = connector;
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
    // slow or unreachable server never delays the counting of faster ones that finished behind it. This
    // removes head-of-line blocking from the *counting* only: with more channels than workers the surplus
    // still queues for a worker, which is what the per-channel timeout bounds.
    CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
    int connected = 0;
    try {
      for (ServerInstance server : servers) {
        for (TableType tableType : TableType.values()) {
          completionService.submit(
              () -> _connector.connect(server, tableType, Math.max(0L, deadlineMs - System.currentTimeMillis())));
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
    long elapsedMs = System.currentTimeMillis() - startMs;
    if (connected < channelCount) {
      LOGGER.warn("Broker pre-connected only {}/{} channel(s) across {} server(s) in {} ms; the rest fall back to the "
          + "lazy connect path", connected, channelCount, servers.size(), elapsedMs);
    } else {
      LOGGER.info("Broker pre-connected {}/{} channel(s) across {} server(s) in {} ms", connected, channelCount,
          servers.size(), elapsedMs);
    }
    return connected;
  }
}
