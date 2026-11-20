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
import java.time.Duration;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class ChannelManagerTest {

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

  /// Refreshing keeps the channel object, which senders may hold, and only drops what it resolved and connected to.
  @Test
  public void testRefreshChannel() {
    ChannelManager channelManager = newChannelManager();
    assertFalse(channelManager.refreshChannel("localhost", 12348));
    ManagedChannel channel = channelManager.getChannel("localhost", 12348);
    try {
      // Nothing listens there: the connect attempt fails, but takes the channel out of IDLE
      channel.getState(true);
      assertTrue(channelManager.refreshChannel("localhost", 12348));
      // A stale connection fails many streams at once: only the first one drops the connection
      assertFalse(channelManager.refreshChannel("localhost", 12348));
      TestUtils.waitForCondition(aVoid -> channel.getState(false) == ConnectivityState.IDLE, 10_000L,
          "The channel did not go back to IDLE");
      assertFalse(channel.isShutdown());
      assertSame(channelManager.getChannel("localhost", 12348), channel);

      // Once a second has passed, the channel can be refreshed again
      channel.getState(true);
      TestUtils.waitForCondition(aVoid -> channelManager.refreshChannel("localhost", 12348), 10_000L,
          "The channel was not refreshed again");
      TestUtils.waitForCondition(aVoid -> channel.getState(false) == ConnectivityState.IDLE, 10_000L,
          "The channel did not go back to IDLE");
    } finally {
      channelManager.shutdown();
    }
  }

  @Test
  public void testRetireChannelAndShutdown() {
    ChannelManager channelManager = newChannelManager();
    assertFalse(channelManager.retireChannel("localhost", 12349));
    ManagedChannel retired = channelManager.getChannel("localhost", 12349);
    assertTrue(channelManager.retireChannel("localhost", 12349));
    assertTrue(retired.isShutdown());
    ManagedChannel replacement = channelManager.getChannel("localhost", 12349);
    assertNotSame(replacement, retired);

    ManagedChannel other = channelManager.getChannel("localhost", 12350);
    channelManager.shutdown();
    assertTrue(replacement.isShutdown());
    assertTrue(other.isShutdown());
  }

  private static ChannelManager newChannelManager() {
    return new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        GrpcKeepAliveConfig.DISABLED);
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
