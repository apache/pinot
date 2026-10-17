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
package org.apache.pinot.controller.api.resources;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class PinotBrokerRestletResourceTest {
  private static final String TENANT_NAME = "DefaultTenant";

  @Test
  public void testTenantBrokerDiscoveryExcludesDrainingBrokers() {
    InstanceConfig activeBroker = brokerInstanceConfig("Broker_host1_8099", "host1");
    InstanceConfig drainingBroker = brokerInstanceConfig("Broker_host2_8099", "host2");
    drainingBroker.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    when(resourceManager.getAllBrokerTenantNames()).thenReturn(Set.of(TENANT_NAME));
    when(resourceManager.getAllInstancesConfigsForBrokerTenant(TENANT_NAME))
        .thenReturn(Set.of(activeBroker, drainingBroker));
    PinotBrokerRestletResource resource = new PinotBrokerRestletResource();
    resource._pinotHelixResourceManager = resourceManager;

    List<InstanceInfo> brokers = resource.getBrokersForTenantV2(TENANT_NAME, null);

    assertEquals(brokers.stream().map(InstanceInfo::getInstanceName).collect(Collectors.toList()),
        List.of(activeBroker.getInstanceName()));
    assertEquals(resource.getBrokersForTenant(TENANT_NAME, null), List.of(activeBroker.getInstanceName()));
  }

  private static InstanceConfig brokerInstanceConfig(String instanceName, String hostName) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(hostName);
    instanceConfig.setPort("8099");
    return instanceConfig;
  }
}
