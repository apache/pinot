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

import org.apache.pinot.query.routing.VirtualServerAddress;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryPlanSerDeUtilsTest {

  @Test
  public void shouldSerializeServer() {
    // Given:
    VirtualServerAddress server = Mockito.mock(VirtualServerAddress.class);
    Mockito.when(server.workerId()).thenReturn(1);
    Mockito.when(server.hostname()).thenReturn("Server_192.987.1.123");
    Mockito.when(server.port()).thenReturn(80);

    // When:
    String serialized = QueryPlanSerDeUtils.addressToProto(server);

    // Then:
    Assert.assertEquals(serialized, "1@Server_192.987.1.123:80");
  }

  @Test
  public void shouldDeserializeServerString() {
    // Given:
    String serverString = "1@Server_192.987.1.123:80";

    // When:
    VirtualServerAddress server = QueryPlanSerDeUtils.protoToAddress(serverString);

    // Then:
    Assert.assertEquals(server.workerId(), 1);
    Assert.assertEquals(server.hostname(), "Server_192.987.1.123");
    Assert.assertEquals(server.port(), 80);
  }
}
