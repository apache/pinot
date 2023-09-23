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
package org.apache.pinot.integration.tests;

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.testng.annotations.Test;


@Test(suiteName = "integration-suite-1", groups = {"integration-suite-1"})
public class MultiStageEngineCustomTenantIntegrationTest extends MultiStageEngineIntegrationTest {
  private static final String TEST_TENANT = "TestTenant";

  @Override
  protected String getBrokerTenant() {
    return TEST_TENANT;
  }

  @Override
  protected String getServerTenant() {
    return TEST_TENANT;
  }

  protected void setupTenants()
      throws IOException {
    createBrokerTenant(getBrokerTenant(), 1);
    createServerTenant(getServerTenant(), 1, 0);
  }

  @Override
  protected Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> properties = super.getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    return properties;
  }
}
