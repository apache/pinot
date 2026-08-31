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

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class ChannelManagerTest {

  @Test
  public void testResetConnectBackoffNoOpForUnknownChannel() {
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        GrpcKeepAliveConfig.DISABLED);
    // Should return false and not throw when no channel exists for the given host/port
    assertFalse(channelManager.resetConnectBackoff("unknown-host", 12345));
  }

  @Test
  public void testResetConnectBackoffNoOpWhenNotInTransientFailure() {
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        GrpcKeepAliveConfig.DISABLED);
    // Create a channel by calling getChannel
    ManagedChannel channel = channelManager.getChannel("localhost", 12345);
    try {
      // Reset backoff should return false since channel is not in TRANSIENT_FAILURE
      assertFalse(channelManager.resetConnectBackoff("localhost", 12345));
      // Channel should still be the same cached instance
      ManagedChannel sameChannel = channelManager.getChannel("localhost", 12345);
      assertSame(sameChannel, channel);
    } finally {
      channel.shutdownNow();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testResetConnectBackoffResetsWhenInTransientFailure()
      throws Exception {
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        GrpcKeepAliveConfig.DISABLED);

    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);

    // Inject the mock channel into _channelMap via reflection
    Field channelMapField = ChannelManager.class.getDeclaredField("_channelMap");
    channelMapField.setAccessible(true);
    ConcurrentHashMap<Pair<String, Integer>, ManagedChannel> channelMap =
        (ConcurrentHashMap<Pair<String, Integer>, ManagedChannel>) channelMapField.get(channelManager);
    channelMap.put(Pair.of("failing-host", 9999), mockChannel);

    assertTrue(channelManager.resetConnectBackoff("failing-host", 9999));
    verify(mockChannel).resetConnectBackoff();
  }

  /// Pins the fail-fast gate added in commit 1d29438dc0 ("Fail fast on invalid gRPC mailbox transport
  /// configuration"): a non-positive `writeBufferLowWaterMarkBytes` must throw at startup rather than
  /// surfacing later as a Netty `WriteBufferWaterMark` constructor failure on the first send.
  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*writeBufferLowWaterMarkBytes must be positive.*")
  public void testConstructorRejectsZeroWriteBufferLowWaterMark() {
    new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        0, GrpcKeepAliveConfig.DISABLED);
  }

  /// Pins the eager `new WriteBufferWaterMark(low, high)` invariant: when `low > high`, Netty's own
  /// constructor throws `IllegalArgumentException`. Constructing the watermark eagerly in
  /// `ChannelManager` (added in 1d29438dc0) is what makes this surface at startup instead of on the
  /// first send to a previously-unseen peer.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorRejectsLowWatermarkAboveHighWatermark() {
    new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        32 * 1024 * 1024,  // high
        64 * 1024 * 1024, // low > high
        GrpcKeepAliveConfig.DISABLED);
  }

  /// Pins that the manager hands its keep-alive policy to every channel it builds.
  ///
  /// gRPC exposes nothing on a built `ManagedChannel` to read the keep-alive settings back, so this asserts the
  /// policy the manager holds; that the policy reaches the transport is covered end to end by
  /// [MailboxChannelKeepAliveTest].
  @Test
  public void testKeepAliveConfigRetained() {
    GrpcKeepAliveConfig keepAlive = new GrpcKeepAliveConfig(30_000, 5_000, true);
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES, keepAlive);
    assertSame(channelManager.getKeepAliveConfig(), keepAlive);
    // Building a channel with keep-alive enabled must not throw: gRPC rejects a non-positive keepAliveTime, and the
    // enabled check that prevents that lives in GrpcKeepAliveConfig rather than at this call site.
    ManagedChannel channel = channelManager.getChannel("localhost", 12346);
    try {
      assertSame(channelManager.getChannel("localhost", 12346), channel);
    } finally {
      channel.shutdownNow();
    }
  }

  /// A disabled policy must leave the builder alone rather than passing `-1` to gRPC, which throws.
  @Test
  public void testDisabledKeepAliveStillBuildsChannel() {
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        GrpcKeepAliveConfig.DISABLED);
    ManagedChannel channel = channelManager.getChannel("localhost", 12347);
    channel.shutdownNow();
  }
}
