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

import java.util.Collections;
import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TenantTagReadinessCallbackTest {

  private static final String CLUSTER_NAME = "testCluster";
  private static final String INSTANCE_ID = "Broker_localhost_8099";

  private HelixManager _helixManager;
  private ConfigAccessor _configAccessor;
  private InstanceConfig _instanceConfig;
  private TenantTagReadinessCallback _callback;

  @BeforeMethod
  public void setUp() {
    _helixManager = mock(HelixManager.class);
    _configAccessor = mock(ConfigAccessor.class);
    _instanceConfig = mock(InstanceConfig.class);

    when(_helixManager.getConfigAccessor()).thenReturn(_configAccessor);
    when(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_ID)).thenReturn(_instanceConfig);

    _callback = new TenantTagReadinessCallback(_helixManager, CLUSTER_NAME, INSTANCE_ID);
  }

  @Test
  public void testReturnsStartingWhenInstanceConfigNotFound() {
    when(_configAccessor.getInstanceConfig(CLUSTER_NAME, INSTANCE_ID)).thenReturn(null);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.STARTING);
    assertThat(_callback.getStatusDescription()).contains("Instance config not found");
  }

  @Test
  public void testReturnsStartingWhenOnlyUntaggedBroker() {
    List<String> tags = Collections.singletonList(Helix.UNTAGGED_BROKER_INSTANCE);
    when(_instanceConfig.getTags()).thenReturn(tags);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.STARTING);
    assertThat(_callback.getStatusDescription()).contains("No valid tenant broker tags");
  }

  @Test
  public void testReturnsGoodWhenValidBrokerTag() {
    List<String> tags = Collections.singletonList("DefaultTenant_BROKER");
    when(_instanceConfig.getTags()).thenReturn(tags);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);
    assertThat(_callback.getStatusDescription()).isEqualTo(ServiceStatus.STATUS_DESCRIPTION_NONE);
  }

  @Test
  public void testStatusIsCachedOnceGood() {
    when(_instanceConfig.getTags()).thenReturn(Collections.singletonList("DefaultTenant_BROKER"));
    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);

    // Change mock to return STARTING conditions
    when(_instanceConfig.getTags()).thenReturn(Collections.emptyList());

    // Should still return GOOD (cached)
    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);
    assertThat(_callback.getStatusDescription()).isEqualTo(ServiceStatus.STATUS_DESCRIPTION_NONE);
  }
}
