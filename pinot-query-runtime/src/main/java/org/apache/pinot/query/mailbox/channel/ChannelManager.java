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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;


/**
 * {@code ChannelManager} manages Grpc send/receive channels.
 *
 * <p>Grpc channels are managed centralized per Pinot component. Channels should be reused across different
 * query/job/stages.
 */
public class ChannelManager {
  /**
   * Map from (hostname, port) to the ManagedChannel with all known channels
   */
  private final ConcurrentHashMap<Pair<String, Integer>, ManagedChannel> _channelMap = new ConcurrentHashMap<>();
  private final TlsConfig _tlsConfig;
  /**
   * The idle timeout for the channel, which cannot be disabled in gRPC.
   *
   * In general we want to prevent the channel from going idle, so that we don't have to re-establish the connection
   * (including TLS negotiation) before sending any message, which increases the latency of the first query sent after a
   * period of inactivity. In order to achieve that, we set the idle timeout to a very large value by default.
   */
  private final Duration _idleTimeout;
  private final int _maxInboundMessageSize;

  public ChannelManager(@Nullable TlsConfig tlsConfig, int maxInboundMessageSize, Duration idleTimeout) {
    _tlsConfig = tlsConfig;
    _maxInboundMessageSize = maxInboundMessageSize;
    _idleTimeout = idleTimeout;
  }

  public ManagedChannel getChannel(String hostname, int port) {
    // TODO: Revisit parameters
    if (_tlsConfig != null) {
      return _channelMap.computeIfAbsent(Pair.of(hostname, port),
          (k) -> {
            NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forAddress(k.getLeft(), k.getRight())
                .maxInboundMessageSize(
                    _maxInboundMessageSize)
                .sslContext(ServerGrpcQueryClient.buildSslContext(_tlsConfig));
            return decorate(channelBuilder).build();
          }
      );
    } else {
      return _channelMap.computeIfAbsent(Pair.of(hostname, port),
          (k) -> {
            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forAddress(k.getLeft(), k.getRight())
                .maxInboundMessageSize(
                    _maxInboundMessageSize)
                .usePlaintext();
            return decorate(channelBuilder).build();
          });
    }
  }

  private ManagedChannelBuilder<?> decorate(ManagedChannelBuilder<?> builder) {
    return builder.idleTimeout(_idleTimeout.getSeconds(), TimeUnit.SECONDS);
  }
}
