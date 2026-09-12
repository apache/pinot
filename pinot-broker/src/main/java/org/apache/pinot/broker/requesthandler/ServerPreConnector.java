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
/// A channel's identity includes the table type, so OFFLINE and REALTIME are **separate** channels
/// (separate sockets, separate handshakes) to the same physical server. The caller supplies the exact
/// (server, table type) pairs to open, derived from what this broker actually routes -- so an offline-only
/// cluster opens no REALTIME channels, and a broker serving one tenant does not connect to another
/// tenant's servers. A table that lands on a server later is left to the lazy connect path (one query pays
/// the connect) rather than pre-warmed here on the chance it appears: the would-be second channel shares
/// nothing with the first, so pre-warming it amortizes nothing. Connecting an already-active channel is a
/// no-op, so this is safe to call more than once.
///
/// Bounded so it can never stall startup: a capped thread pool, a per-channel connect bound derived from
/// the remaining budget, and the caller's `deadlineMs` as the single release bound. There is no early
/// release: [#preConnect] waits for each channel to resolve (connect or fail) up to the deadline, then
/// returns. That keeps readiness honest -- the broker is released once its channels are actually warm, or
/// the budget is spent -- rather than guessing from a quiet period that the rest are stuck, which cannot
/// tell a slow-but-healthy connect from a dead one and so can release with healthy channels still cold. A
/// failed or unreachable connect is logged and skipped and never stops the others from being waited for;
/// the deadline is the only thing that ends the wait early. This class is stateless and thread-safe.
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
  /// behind the workers, so the deadline alone must not be what stops a stuck connect. That is why
  /// [ChannelConnector] takes a per-channel timeout.
  @VisibleForTesting
  static final int MAX_CONNECT_THREADS = 16;

  /// Opens one broker-to-server channel. Implementations must bound their own wait by `timeoutMs` and
  /// must not throw; the return value reports whether the channel is connected.
  @FunctionalInterface
  public interface ChannelConnector {
    boolean connect(ServerInstance serverInstance, TableType tableType, long timeoutMs);
  }

  /// A (server, table type) channel to open. The table type is part of the channel identity: OFFLINE and
  /// REALTIME are separate channels (separate sockets) to the same physical server.
  public record ChannelTarget(ServerInstance serverInstance, TableType tableType) {
  }

  private final Supplier<Collection<ChannelTarget>> _targetsSupplier;
  private final ChannelConnector _connector;

  /// @param targetsSupplier supplies the (server, table type) channels to open, evaluated once per
  ///     [#preConnect] call after the caller has ensured routing is built. Derive these from routing so
  ///     only channels this broker actually uses are opened. Must return a non-null collection and must
  ///     not throw (it is evaluated before the failure-handling loop); the production supplier reads
  ///     routing, which cannot do either.
  /// @param connector opens the channel for one (server, table type) within a timeout
  public ServerPreConnector(Supplier<Collection<ChannelTarget>> targetsSupplier, ChannelConnector connector) {
    _targetsSupplier = targetsSupplier;
    _connector = connector;
  }

  /// Opens the supplied (server, table type) channels in parallel, bounded by `deadlineMs` (an absolute
  /// [System#currentTimeMillis] value). Returns the number of channels connected by the deadline. A channel
  /// still connecting when the deadline passes keeps warming on its daemon thread and is published for the
  /// first query to reuse; it is simply not counted. Never throws: a channel that fails or times out is
  /// logged and skipped, and never stops the others from being waited for.
  public int preConnect(long deadlineMs) {
    // Snapshot the target view once. The supplier may derive from a live routing view that another thread
    // updates during startup; snapshotting keeps the channel count consistent with the tasks actually
    // submitted below, so we never poll for phantom channels or under-count real ones.
    List<ChannelTarget> targets = new ArrayList<>(_targetsSupplier.get());
    if (targets.isEmpty() || System.currentTimeMillis() >= deadlineMs) {
      return 0;
    }
    long startMs = System.currentTimeMillis();
    int channelCount = targets.size();
    ExecutorService executor = Executors.newFixedThreadPool(Math.min(channelCount, MAX_CONNECT_THREADS),
        new ThreadFactoryBuilder().setNameFormat("broker-preconnect-%d").setDaemon(true).build());
    // A completion service hands channels back in the order they finish, not the order submitted, so a
    // slow or unreachable server never delays the counting of faster ones that finished behind it. This
    // removes head-of-line blocking from the *counting* only: with more channels than workers the surplus
    // still queues for a worker, which is what the per-channel timeout bounds.
    CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
    int connected = 0;
    try {
      for (ChannelTarget target : targets) {
        completionService.submit(() -> _connector.connect(target.serverInstance(), target.tableType(),
            Math.max(0L, deadlineMs - System.currentTimeMillis())));
      }
      for (int i = 0; i < channelCount; i++) {
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          break;
        }
        try {
          // Wait up to the whole remaining budget for the next channel. A completed channel returns
          // immediately, so a fully healthy cluster is released as soon as its last channel is up -- not at
          // the deadline. The only thing this actually blocks on is a channel that never completes (an
          // unreachable server that black-holes to its own connect timeout); the deadline is the single
          // bound on that, and one such server costs at most the budget, never the others' warm-up.
          Future<Boolean> future = completionService.poll(remainingMs, TimeUnit.MILLISECONDS);
          if (future == null) {
            LOGGER.info("Pre-connect budget elapsed with {}/{} channel(s) still outstanding; releasing startup "
                + "and leaving them to the lazy connect path", channelCount - i, channelCount);
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
          // A server that is unreachable or itself restarting must not stop us waiting for the others.
          LOGGER.debug("Pre-connect did not complete for one channel", e);
        }
      }
    } finally {
      // shutdown(), not shutdownNow(): a channel still connecting past the deadline is on a daemon thread.
      // Interrupting a worker parked in connect().sync() abandons a ChannelFuture that can still complete,
      // leaking a socket nobody references or closes. Letting the workers run means a late channel still
      // warms in the background and is published for the first query to reuse, and each task is already
      // bounded by its own deadline-derived timeout, so none outlives the budget.
      executor.shutdown();
    }
    long elapsedMs = System.currentTimeMillis() - startMs;
    if (connected < channelCount) {
      LOGGER.warn("Broker pre-connected {}/{} channel(s) in {} ms; the rest fall back to the lazy connect path",
          connected, channelCount, elapsedMs);
    } else {
      LOGGER.info("Broker pre-connected {}/{} channel(s) in {} ms", connected, channelCount, elapsedMs);
    }
    return connected;
  }
}
