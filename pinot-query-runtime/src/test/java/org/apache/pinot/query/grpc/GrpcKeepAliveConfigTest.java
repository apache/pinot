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

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class GrpcKeepAliveConfigTest {

  @Test
  public void testDisabledWhenTimeNotPositive() {
    assertFalse(GrpcKeepAliveConfig.DISABLED.isEnabled());
    assertFalse(new GrpcKeepAliveConfig(0, 30_000, false).isEnabled());
    assertFalse(new GrpcKeepAliveConfig(-1, 30_000, false).isEnabled());
  }

  @Test
  public void testEnabledExposesSettings() {
    GrpcKeepAliveConfig config = new GrpcKeepAliveConfig(300_000, 30_000, true);
    assertTrue(config.isEnabled());
    assertEquals(config.timeMs(), 300_000);
    assertEquals(config.timeoutMs(), 30_000);
    assertTrue(config.withoutCalls());
  }

  /// A zero or negative timeout is only rejected when keep-alive is on: gRPC would throw on it, while
  /// [GrpcKeepAliveConfig#DISABLED] must stay constructible whatever the timeout is.
  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*keepAliveTimeoutMs must be positive.*")
  public void testRejectsNonPositiveTimeoutWhenEnabled() {
    new GrpcKeepAliveConfig(300_000, 0, false);
  }

  @Test
  public void testDisabledAllowsAnyTimeout() {
    assertFalse(new GrpcKeepAliveConfig(-1, 0, false).isEnabled());
  }

  /// The whole point of routing both call sites through `configure`: a disabled policy must not reach gRPC, which
  /// rejects a non-positive `keepAliveTime` with an `IllegalArgumentException`.
  @Test
  public void testConfigureIsNoOpWhenDisabled() {
    NettyChannelBuilder builder = NettyChannelBuilder.forAddress("localhost", 12345).usePlaintext();
    assertSame(GrpcKeepAliveConfig.DISABLED.configure(builder), builder);
    builder.build().shutdownNow();
  }

  @Test
  public void testConfigureAppliesWhenEnabled() {
    NettyChannelBuilder builder = NettyChannelBuilder.forAddress("localhost", 12345).usePlaintext();
    assertSame(new GrpcKeepAliveConfig(30_000, 5_000, true).configure(builder), builder);
    builder.build().shutdownNow();
  }

  /// Defaults must be the ones an operator reads in the docs, resolved in one place so both the Java
  /// mailbox and any other engine maintaining its own mailbox channels see the same values.
  @Test
  public void testForMailboxChannelsDefaults() {
    GrpcKeepAliveConfig config = GrpcKeepAliveConfig.forMailboxChannels(new PinotConfiguration(Map.of()));
    assertTrue(config.isEnabled(), "mailbox channels must ping by default");
    assertEquals(config.timeMs(), MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIME_MS);
    assertEquals(config.timeoutMs(), MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIMEOUT_MS);
    // Off by default: pings without an active call are refused by a peer left at Netty's own default.
    assertFalse(config.withoutCalls());
  }

  /// The default interval must not exceed what an un-upgraded peer permits, or an upgraded instance
  /// would have its mailbox channels torn down with `GOAWAY(ENHANCE_YOUR_CALM)` during a rolling
  /// upgrade.
  @Test
  public void testDefaultIntervalIsSafeAgainstNettyServerDefault() {
    assertTrue(MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIME_MS
            >= MultiStageQueryRunner.DEFAULT_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS,
        "client keep-alive time must be >= the permit enforced by peers");
  }

  @Test
  public void testForMailboxChannelsOverrides() {
    GrpcKeepAliveConfig config = GrpcKeepAliveConfig.forMailboxChannels(new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIME_MS, 30_000,
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIMEOUT_MS, 5_000,
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS, true)));
    assertEquals(config.timeMs(), 30_000);
    assertEquals(config.timeoutMs(), 5_000);
    assertTrue(config.withoutCalls());
  }

  @Test
  public void testForMailboxChannelsCanTurnKeepAliveOff() {
    GrpcKeepAliveConfig config = GrpcKeepAliveConfig.forMailboxChannels(new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIME_MS, -1)));
    assertFalse(config.isEnabled());
  }
}
