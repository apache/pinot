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
package org.apache.pinot.common.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class NettyConfigTest {

  @Test
  public void testDefaults() {
    NettyConfig config = NettyConfig.extractNettyConfig(new PinotConfiguration(), Broker.BROKER_NETTY_PREFIX);
    assertFalse(config.isNativeTransportsEnabled());
    assertEquals(config.getWriteTimeoutMs(), NettyConfig.DEFAULT_WRITE_TIMEOUT_MS);
    assertEquals(config.getTcpUserTimeoutMs(), NettyConfig.DEFAULT_TCP_USER_TIMEOUT_MS);
    assertEquals(config.getWriteBufferHighWaterMarkBytes(), NettyConfig.DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK_BYTES);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), NettyConfig.DEFAULT_WRITE_BUFFER_LOW_WATER_MARK_BYTES);

    // A bare NettyConfig, which is what the transport falls back to when none is passed, carries the same defaults
    NettyConfig bare = new NettyConfig();
    assertEquals(bare.getWriteTimeoutMs(), config.getWriteTimeoutMs());
    assertEquals(bare.getTcpUserTimeoutMs(), config.getTcpUserTimeoutMs());
    assertEquals(bare.getWriteBufferHighWaterMarkBytes(), config.getWriteBufferHighWaterMarkBytes());
    assertEquals(bare.getWriteBufferLowWaterMarkBytes(), config.getWriteBufferLowWaterMarkBytes());

    // The defaults themselves must be a valid watermark pair, or the broker would fail to start out of the box
    assertTrue(NettyConfig.DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK_BYTES > 0);
    int defaultLow = NettyConfig.DEFAULT_WRITE_BUFFER_LOW_WATER_MARK_BYTES;
    int defaultHigh = NettyConfig.DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK_BYTES;
    assertTrue(defaultLow >= 0 && defaultLow <= defaultHigh);
  }

  /// Pins the broker config keys: these are the names operators set.
  @Test
  public void testExtractBrokerKeys() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("pinot.broker.netty.native.transports.enabled", true);
    properties.put("pinot.broker.netty.write.timeout.ms", 2_500);
    properties.put("pinot.broker.netty.tcp.user.timeout.ms", 3_000);
    properties.put("pinot.broker.netty.write.buffer.high.water.mark.bytes", 1024 * 1024);
    properties.put("pinot.broker.netty.write.buffer.low.water.mark.bytes", 512 * 1024);
    PinotConfiguration pinotConfig = new PinotConfiguration(properties);

    NettyConfig config = NettyConfig.extractNettyConfig(pinotConfig, Broker.BROKER_NETTY_PREFIX);
    assertTrue(config.isNativeTransportsEnabled());
    assertEquals(config.getWriteTimeoutMs(), 2_500L);
    assertEquals(config.getTcpUserTimeoutMs(), 3_000);
    assertEquals(config.getWriteBufferHighWaterMarkBytes(), 1024 * 1024);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), 512 * 1024);

    // Keys are namespaced: the server-side extraction does not pick up the broker's values
    NettyConfig serverConfig = NettyConfig.extractNettyConfig(pinotConfig, Server.SERVER_NETTY_PREFIX);
    assertFalse(serverConfig.isNativeTransportsEnabled());
    assertEquals(serverConfig.getWriteTimeoutMs(), NettyConfig.DEFAULT_WRITE_TIMEOUT_MS);
    assertEquals(serverConfig.getTcpUserTimeoutMs(), NettyConfig.DEFAULT_TCP_USER_TIMEOUT_MS);
  }

  @Test
  public void testExplicitDefaultsAreUsedWhenKeysAreAbsent() {
    NettyConfig defaults = new NettyConfig();
    defaults.setNativeTransportsEnabled(true);
    defaults.setWriteTimeoutMs(1L);
    defaults.setTcpUserTimeoutMs(2);
    defaults.setWriteBufferWaterMarks(3, 4);

    NettyConfig config =
        NettyConfig.extractNettyConfig(new PinotConfiguration(), Broker.BROKER_NETTY_PREFIX, defaults);
    assertTrue(config.isNativeTransportsEnabled());
    assertEquals(config.getWriteTimeoutMs(), 1L);
    assertEquals(config.getTcpUserTimeoutMs(), 2);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), 3);
    assertEquals(config.getWriteBufferHighWaterMarkBytes(), 4);
  }

  /// Non-positive timeouts are the documented way to turn each check off, so they must be accepted as-is.
  @Test
  public void testNonPositiveTimeoutsDisable() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("pinot.broker.netty.write.timeout.ms", 0);
    properties.put("pinot.broker.netty.tcp.user.timeout.ms", -1);
    NettyConfig config =
        NettyConfig.extractNettyConfig(new PinotConfiguration(properties), Broker.BROKER_NETTY_PREFIX);
    assertEquals(config.getWriteTimeoutMs(), 0L);
    assertEquals(config.getTcpUserTimeoutMs(), -1);
  }

  @Test
  public void testWaterMarkValidation() {
    NettyConfig config = new NettyConfig();
    // Low may equal high, and may be zero
    config.setWriteBufferWaterMarks(10, 10);
    config.setWriteBufferWaterMarks(0, 10);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), 0);
    assertEquals(config.getWriteBufferHighWaterMarkBytes(), 10);

    assertThrows(IllegalArgumentException.class, () -> config.setWriteBufferWaterMarks(11, 10));
    assertThrows(IllegalArgumentException.class, () -> config.setWriteBufferWaterMarks(0, 0));
    assertThrows(IllegalArgumentException.class, () -> config.setWriteBufferWaterMarks(-1, 10));
    // A rejected pair leaves the previous values in place
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), 0);
    assertEquals(config.getWriteBufferHighWaterMarkBytes(), 10);

    // The same validation applies to values read from the config
    Map<String, Object> properties = new HashMap<>();
    properties.put("pinot.broker.netty.write.buffer.high.water.mark.bytes", 100);
    properties.put("pinot.broker.netty.write.buffer.low.water.mark.bytes", 200);
    assertThrows(IllegalArgumentException.class,
        () -> NettyConfig.extractNettyConfig(new PinotConfiguration(properties), Broker.BROKER_NETTY_PREFIX));
  }

  /// Lowering only the high watermark must not fail startup because the default low watermark sits above it.
  @Test
  public void testLoweredHighWaterMarkPullsTheDefaultLowWaterMarkDown() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("pinot.broker.netty.write.buffer.high.water.mark.bytes", 1024 * 1024);
    NettyConfig config =
        NettyConfig.extractNettyConfig(new PinotConfiguration(properties), Broker.BROKER_NETTY_PREFIX);
    assertEquals(config.getWriteBufferHighWaterMarkBytes(), 1024 * 1024);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), 512 * 1024);

    // A high watermark above the default leaves the default low watermark alone
    properties.put("pinot.broker.netty.write.buffer.high.water.mark.bytes", 64 * 1024 * 1024);
    config = NettyConfig.extractNettyConfig(new PinotConfiguration(properties), Broker.BROKER_NETTY_PREFIX);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), NettyConfig.DEFAULT_WRITE_BUFFER_LOW_WATER_MARK_BYTES);

    // An explicit low watermark is always honoured
    properties.put("pinot.broker.netty.write.buffer.low.water.mark.bytes", 1024);
    config = NettyConfig.extractNettyConfig(new PinotConfiguration(properties), Broker.BROKER_NETTY_PREFIX);
    assertEquals(config.getWriteBufferLowWaterMarkBytes(), 1024);
  }
}
