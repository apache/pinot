/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.util.TestUtils;
import javax.annotation.Nullable;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test sets up the Pinot cluster and ensures the sanity/consistency of cluster after modifications are performed
 * on the cluster.
 */
public class ClusterSanityTest extends ClusterTest {
  private static final String TENANT_NAME = ControllerTenantNameBuilder.DEFAULT_TENANT_NAME;
  private static final String BROKER_TENANT_NAME =
      ControllerTenantNameBuilder.getBrokerTenantNameForTenant(TENANT_NAME);
  private static final String TABLE_NAME = "ClusterSanityTestTable";
  private static final int INITIAL_NUM_BROKERS = 2;
  private static final int NEW_NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    startBrokers(INITIAL_NUM_BROKERS);
    startServers(NUM_SERVERS);
    addOfflineTable(null, null, -1, null, TENANT_NAME, TENANT_NAME, TABLE_NAME, SegmentVersion.v1);
  }

  /**
   * This test ensures that the cluster is in a consistent state after scaling down the brokers.
   *
   * @throws Exception
   */
  @Test
  public void testBrokerScaleDown() throws Exception {
    Tenant tenant =
        new Tenant.TenantBuilder(TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(NEW_NUM_BROKERS).build();

    // Send the 'put' (instead of 'post') request, that updates the tenants instead of creating
    JSONObject request = tenant.toJSON();
    sendPutRequest(_controllerRequestURLBuilder.forBrokerTenantCreate(), request.toString());

    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        return _helixAdmin.getInstancesInClusterWithTag(_clusterName, BROKER_TENANT_NAME).size() == 1;
      }
    }, 60_000L, "Failed to get in a consistent state after scaling down the brokers");
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropOfflineTable(TABLE_NAME);
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }
}
