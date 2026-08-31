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
package org.apache.pinot.query.grpc;

import com.google.common.base.Preconditions;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/// Immutable gRPC client keep-alive settings for the multi-stage engine's internal channels.
///
/// Keep-alive makes a *silently* unreachable peer observable. A peer whose process is gone sends a `RST`, and gRPC
/// fails the channel immediately; a peer whose kernel is hung, or that sits behind a one-way partition, sends
/// nothing at all. Without a transport-level ping such a channel stays in `READY` indefinitely, and every RPC
/// issued on it parks until its own deadline. Since channels are cached per peer, that outlives the query that
/// first hit it: the cached channel keeps pointing at a dead peer, and because gRPC only re-resolves DNS when a
/// transport is dropped, even a peer that has since come back at a new address is never reached again.
///
/// Applies to the MSE's own channels only. The keep-alive settings for the user-facing gRPC query service live in
/// [org.apache.pinot.common.config.GrpcConfig].
///
/// @param timeMs interval between keep-alive pings, in milliseconds; keep-alive is disabled when not positive.
///               gRPC clamps this up to its own 10s floor, so a smaller value has no effect
/// @param timeoutMs how long a ping may go unanswered before the transport is declared dead. Must be positive when
///                  keep-alive is enabled
/// @param withoutCalls whether to ping while the connection carries no active RPC. Requires the peer to permit it
///                     (`permitKeepAliveWithoutCalls`), otherwise the peer answers with `GOAWAY(ENHANCE_YOUR_CALM)`
public record GrpcKeepAliveConfig(int timeMs, int timeoutMs, boolean withoutCalls) {
  /// No keep-alive pings. gRPC's own default, and what every MSE channel did before keep-alive was configurable.
  public static final GrpcKeepAliveConfig DISABLED = new GrpcKeepAliveConfig(-1, 30_000, false);

  /// Reads the policy for the multi-stage engine's **mailbox** channels from `config`.
  ///
  /// The single place these three keys and their defaults are interpreted, so that anything opening a
  /// mailbox channel — including an alternative execution engine that maintains its own channels —
  /// resolves exactly what [org.apache.pinot.query.mailbox.channel.ChannelManager] would, and cannot
  /// drift from it if a default changes.
  ///
  /// The broker dispatch channel has its own keys and reads them at its own call site
  /// ([org.apache.pinot.broker.requesthandler.MultiStageBrokerRequestHandler]), because it also has to
  /// hand them to a [org.apache.pinot.query.service.dispatch.QueryDispatcher] overload that predates
  /// this type.
  public static GrpcKeepAliveConfig forMailboxChannels(PinotConfiguration config) {
    return new GrpcKeepAliveConfig(
        config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIME_MS,
            CommonConstants.MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIME_MS),
        config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIMEOUT_MS,
            CommonConstants.MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIMEOUT_MS),
        config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS,
            CommonConstants.MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS));
  }

  public GrpcKeepAliveConfig {
    if (timeMs > 0) {
      Preconditions.checkArgument(timeoutMs > 0,
          "keepAliveTimeoutMs must be positive when keep-alive is enabled, got: %s", timeoutMs);
    }
  }

  /// Applies these settings to `builder`, leaving it untouched when keep-alive is disabled.
  ///
  /// Centralizing the enabled check here is what keeps a caller from configuring a `keepAliveTime` of `-1`, which
  /// gRPC rejects, instead of leaving the ping off.
  public NettyChannelBuilder configure(NettyChannelBuilder builder) {
    if (isEnabled()) {
      builder.keepAliveTime(timeMs, TimeUnit.MILLISECONDS)
          .keepAliveTimeout(timeoutMs, TimeUnit.MILLISECONDS)
          .keepAliveWithoutCalls(withoutCalls);
    }
    return builder;
  }

  public boolean isEnabled() {
    return timeMs > 0;
  }

  /// Renders as the mailbox and dispatch startup logs show it. Kept instead of the generated `toString` so a
  /// disabled policy reads as such rather than as a `-1` interval.
  @Override
  public String toString() {
    return isEnabled()
        ? "keepAlive[timeMs=" + timeMs + ", timeoutMs=" + timeoutMs + ", withoutCalls=" + withoutCalls + "]"
        : "keepAlive[disabled]";
  }
}
