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
package org.apache.pinot.broker.broker.helix;

import java.util.Map;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.broker.routing.manager.RemoteClusterBrokerRoutingManager;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/// Verifies that multi-cluster failure-detector notifications update every applicable routing manager.
public class MultiClusterHelixBrokerStarterTest {

  @Test
  public void testFailureDetectorNotificationsAreForwardedToAllRoutingManagers() {
    BrokerRoutingManager localManager = mock(BrokerRoutingManager.class);
    RemoteClusterBrokerRoutingManager remoteManager0 = mock(RemoteClusterBrokerRoutingManager.class);
    RemoteClusterBrokerRoutingManager remoteManager1 = mock(RemoteClusterBrokerRoutingManager.class);
    MultiClusterHelixBrokerStarter starter = new MultiClusterHelixBrokerStarter();
    starter._routingManager = localManager;
    starter._remoteRoutingManagers = Map.of("remote0", remoteManager0, "remote1", remoteManager1);

    starter.includeServerToRouting("server0");
    starter.excludeServerFromRouting("server1");

    verify(localManager).includeServerToRouting("server0");
    verify(remoteManager0).includeServerToRouting("server0");
    verify(remoteManager1).includeServerToRouting("server0");
    verify(localManager).excludeServerFromRouting("server1");
    verify(remoteManager0).excludeServerFromRouting("server1");
    verify(remoteManager1).excludeServerFromRouting("server1");
  }

  @Test
  public void testFailureDetectorNotificationsBeforeRemoteManagerInitialization() {
    BrokerRoutingManager localManager = mock(BrokerRoutingManager.class);
    MultiClusterHelixBrokerStarter starter = new MultiClusterHelixBrokerStarter();
    starter._routingManager = localManager;

    starter.includeServerToRouting("server0");
    starter.excludeServerFromRouting("server1");

    verify(localManager).includeServerToRouting("server0");
    verify(localManager).excludeServerFromRouting("server1");
  }
}
