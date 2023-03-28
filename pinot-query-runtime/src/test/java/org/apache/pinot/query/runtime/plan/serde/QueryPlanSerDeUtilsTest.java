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

package org.apache.pinot.query.runtime.plan.serde;

import java.util.Arrays;
import org.apache.pinot.query.routing.VirtualServer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryPlanSerDeUtilsTest {

  @Test
  public void shouldSerializeServer() {
    // Given:
    VirtualServer server = Mockito.mock(VirtualServer.class);
    Mockito.when(server.getPartitionIds()).thenReturn(Arrays.asList(0, 1));
    Mockito.when(server.getHostname()).thenReturn("Server_192.987.1.123");
    Mockito.when(server.getPort()).thenReturn(80);
    Mockito.when(server.getGrpcPort()).thenReturn(10);
    Mockito.when(server.getQueryServicePort()).thenReturn(20);
    Mockito.when(server.getQueryMailboxPort()).thenReturn(30);

    // When:
    String serialized = QueryPlanSerDeUtils.instanceToString(server);

    // Then:
    Assert.assertEquals(serialized, "[0,1]@Server_192.987.1.123:80(10:20:30)");
  }

  @Test
  public void shouldDeserializeServerString() {
    // Given:
    String serverString = "[0,1]@Server_192.987.1.123:80(10:20:30)";

    // When:
    VirtualServer server = QueryPlanSerDeUtils.stringToInstance(serverString);

    // Then:
    Assert.assertEquals(server.getPartitionIds(), Arrays.asList(0, 1));
    Assert.assertEquals(server.getHostname(), "Server_192.987.1.123");
    Assert.assertEquals(server.getPort(), 80);
    Assert.assertEquals(server.getGrpcPort(), 10);
    Assert.assertEquals(server.getQueryServicePort(), 20);
    Assert.assertEquals(server.getQueryMailboxPort(), 30);
  }
}
