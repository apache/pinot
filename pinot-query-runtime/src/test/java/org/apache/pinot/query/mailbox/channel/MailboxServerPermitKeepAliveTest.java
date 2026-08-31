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

import java.util.Map;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Pins the keep-alive enforcement the mailbox server applies to its peers.
///
/// The two sides are coupled: a peer pinging faster than `permitKeepAliveTime` has its pings counted as "bad" and,
/// past gRPC's strike threshold, is answered with `GOAWAY(ENHANCE_YOUR_CALM)` — dropping the mailbox channel
/// mid-query. Both halves therefore have to be tunable, and the defaults have to match Netty's own so that a peer
/// still on the default client keep-alive time is never punished by an upgraded one.
public class MailboxServerPermitKeepAliveTest {

  @Test
  public void testPermitKeepAliveDefaultsMatchNettyDefaults() {
    GrpcMailboxServer server = newServer(new PinotConfiguration(Map.of()));
    assertEquals(server.getPermitKeepAliveTimeMs(),
        MultiStageQueryRunner.DEFAULT_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS);
    assertFalse(server.isPermitKeepAliveWithoutCalls());
  }

  /// Values an operator would use to tune detection of a silent peer down from minutes to seconds. Both the client
  /// keep-alive time and this permit have to move together, on every instance.
  @Test
  public void testPermitKeepAliveOverridesPickedUpFromConfig() {
    GrpcMailboxServer server = newServer(new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS, 30_000,
        MultiStageQueryRunner.KEY_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS, true)));
    assertEquals(server.getPermitKeepAliveTimeMs(), 30_000);
    assertTrue(server.isPermitKeepAliveWithoutCalls());
  }

  /// A non-positive permit must leave Netty's own default in place rather than being passed to gRPC, which rejects
  /// it. Reaching construction at all is the assertion.
  @Test
  public void testNonPositivePermitLeavesNettyDefault() {
    GrpcMailboxServer server = newServer(new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS, -1)));
    assertEquals(server.getPermitKeepAliveTimeMs(), -1);
  }

  /// Builds the server without starting it: nothing binds a port until `start()`, and the values under test are all
  /// resolved in the constructor.
  private static GrpcMailboxServer newServer(PinotConfiguration config) {
    MailboxService mailboxService = new MailboxService(
        "localhost", QueryTestUtils.getAvailablePort(), InstanceType.BROKER, config);
    return new GrpcMailboxServer(mailboxService, config, null, null, null);
  }
}
