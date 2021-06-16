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
package org.apache.pinot.common.utils.helix;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HelixHelperTest {
  @Test
  public void testUpdateInstanceHostNamePort() {
    HelixManager mockedManager = Mockito.mock(HelixManager.class);
    HelixDataAccessor mockedAccessor = Mockito.mock(HelixDataAccessor.class);
    HelixAdmin mockedAdmin = Mockito.mock(HelixAdmin.class);
    Mockito.when(mockedManager.getHelixDataAccessor()).thenReturn(mockedAccessor);
    Mockito.when(mockedManager.getClusterManagmentTool()).thenReturn(mockedAdmin);
    final String clusterName = "amazing_cluster";
    final String instanceId = "some_unique_id";
    InstanceConfig targetConfig = new InstanceConfig(instanceId);
    Mockito.when(mockedAdmin.getInstanceConfig(clusterName, instanceId)).thenReturn(targetConfig);
    Mockito.when(mockedAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(clusterName));
    Mockito.when(mockedAccessor.setProperty(Mockito.any(PropertyKey.class), Mockito.eq(targetConfig))).thenReturn(true);
    HelixHelper.updateInstanceHostNamePort(mockedManager, clusterName, instanceId, "strange.host.com", 234);
    Mockito.verify(mockedAccessor).setProperty(Mockito.any(PropertyKey.class), Mockito.eq(targetConfig));
    Assert.assertEquals("strange.host.com", targetConfig.getHostName());
    Assert.assertEquals("234", targetConfig.getPort());

  }
}
