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
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365));
    // Should return false and not throw when no channel exists for the given host/port
    assertFalse(channelManager.resetConnectBackoff("unknown-host", 12345));
  }

  @Test
  public void testResetConnectBackoffNoOpWhenNotInTransientFailure() {
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365));
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
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365));

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
}
