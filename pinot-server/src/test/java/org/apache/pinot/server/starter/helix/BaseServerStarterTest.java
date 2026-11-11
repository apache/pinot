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
package org.apache.pinot.server.starter.helix;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.NetUtils;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;

public class BaseServerStarterTest {
  @Test
  public void testConfiguredServerHostDoesNotResolveDefaultHost()
      throws Exception {
    PinotConfiguration serverConf = new PinotConfiguration();
    serverConf.setProperty(Helix.KEY_OF_SERVER_NETTY_HOST, "pinot-server-node1");

    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(NetUtils::getHostAddress).thenThrow(new AssertionError("Should not resolve host address"));
      netUtils.when(NetUtils::getHostnameOrAddress).thenThrow(new AssertionError("Should not resolve hostname"));

      assertEquals(BaseServerStarter.getServerHostname(serverConf), "pinot-server-node1");

      netUtils.verify(NetUtils::getHostAddress, never());
      netUtils.verify(NetUtils::getHostnameOrAddress, never());
    }
  }

  @Test
  public void testServerHostFallsBackToHostAddress()
      throws Exception {
    PinotConfiguration serverConf = new PinotConfiguration();

    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(NetUtils::getHostAddress).thenReturn("10.0.0.1");

      assertEquals(BaseServerStarter.getServerHostname(serverConf), "10.0.0.1");

      netUtils.verify(NetUtils::getHostAddress);
      netUtils.verify(NetUtils::getHostnameOrAddress, never());
    }
  }

  @Test
  public void testServerHostFallsBackToHostnameWhenConfigured()
      throws Exception {
    PinotConfiguration serverConf = new PinotConfiguration();
    serverConf.setProperty(Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, true);

    try (MockedStatic<NetUtils> netUtils = mockStatic(NetUtils.class)) {
      netUtils.when(NetUtils::getHostnameOrAddress).thenReturn("pinot-server-node1");

      assertEquals(BaseServerStarter.getServerHostname(serverConf), "pinot-server-node1");

      netUtils.verify(NetUtils::getHostnameOrAddress);
      netUtils.verify(NetUtils::getHostAddress, never());
    }
  }
}
