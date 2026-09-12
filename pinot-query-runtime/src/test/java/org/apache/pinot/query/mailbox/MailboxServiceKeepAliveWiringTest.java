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
package org.apache.pinot.query.mailbox;

import java.util.Map;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Pins the hop from configuration to transport: the policy [MailboxService] resolves must be the one
/// its [org.apache.pinot.query.mailbox.channel.ChannelManager] hands to every channel.
///
/// Asserting the parsed values alone cannot show this. A `MailboxService` that resolved the config
/// correctly and then passed [GrpcKeepAliveConfig#DISABLED] to the channel manager would satisfy every
/// parsing test while shipping channels with no keep-alive at all — the exact defect this change exists
/// to prevent.
public class MailboxServiceKeepAliveWiringTest {

  @Test
  public void testResolvedPolicyReachesTheChannelManager() {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIME_MS, 45_000,
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIMEOUT_MS, 7_000,
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS, true));
    MailboxService mailboxService = newMailboxService(config);
    GrpcKeepAliveConfig applied = mailboxService.getChannelManager().getKeepAliveConfig();
    assertEquals(applied, GrpcKeepAliveConfig.forMailboxChannels(config),
        "the channel manager must get exactly the policy resolved from config");
    assertEquals(applied.timeMs(), 45_000);
    assertEquals(applied.timeoutMs(), 7_000);
    assertTrue(applied.withoutCalls());
  }

  @Test
  public void testDefaultPolicyReachesTheChannelManager() {
    MailboxService mailboxService = newMailboxService(new PinotConfiguration(Map.of()));
    GrpcKeepAliveConfig applied = mailboxService.getChannelManager().getKeepAliveConfig();
    assertTrue(applied.isEnabled(), "mailbox channels must ping by default");
    assertEquals(applied.timeMs(), MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIME_MS);
    assertEquals(applied.timeoutMs(), MultiStageQueryRunner.DEFAULT_CHANNEL_KEEP_ALIVE_TIMEOUT_MS);
    assertFalse(applied.withoutCalls());
  }

  /// Disabling must reach the transport too, so an operator turning keep-alive off gets channels
  /// without pings rather than channels with the defaults.
  @Test
  public void testDisabledPolicyReachesTheChannelManager() {
    MailboxService mailboxService = newMailboxService(new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_CHANNEL_KEEP_ALIVE_TIME_MS, -1)));
    assertFalse(mailboxService.getChannelManager().getKeepAliveConfig().isEnabled());
  }

  /// Constructed but never started: the channel manager and its policy are resolved in the
  /// constructor, so no port needs binding.
  private static MailboxService newMailboxService(PinotConfiguration config) {
    return new MailboxService("localhost", QueryTestUtils.getAvailablePort(), InstanceType.SERVER, config);
  }
}
