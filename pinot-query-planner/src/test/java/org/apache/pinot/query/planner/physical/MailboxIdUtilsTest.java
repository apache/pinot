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
package org.apache.pinot.query.planner.physical;

import java.util.List;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.RoutingInfo;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class MailboxIdUtilsTest {

  /// Each sending mailbox gets the identity of the receiver it is meant for, or 0 when the plan does not say.
  @Test
  public void testToRoutingInfosCarriesReceiverInstanceIdHash() {
    List<RoutingInfo> routingInfos = MailboxIdUtils.toRoutingInfos(7L, 2, 3, 1,
        List.of(new MailboxInfo("host1", 8101, List.of(0, 1), 1234L), new MailboxInfo("host2", 8101, List.of(2))));

    assertEquals(routingInfos.size(), 3);
    assertEquals(routingInfos.get(0).getHostname(), "host1");
    assertEquals(routingInfos.get(0).getMailboxId(), MailboxIdUtils.toMailboxId(7L, 2, 3, 1, 0));
    assertEquals(routingInfos.get(0).getReceiverInstanceIdHash(), 1234L);
    assertEquals(routingInfos.get(1).getMailboxId(), MailboxIdUtils.toMailboxId(7L, 2, 3, 1, 1));
    assertEquals(routingInfos.get(1).getReceiverInstanceIdHash(), 1234L);
    assertEquals(routingInfos.get(2).getHostname(), "host2");
    assertEquals(routingInfos.get(2).getMailboxId(), MailboxIdUtils.toMailboxId(7L, 2, 3, 1, 2));
    assertEquals(routingInfos.get(2).getReceiverInstanceIdHash(), 0L);
  }
}
