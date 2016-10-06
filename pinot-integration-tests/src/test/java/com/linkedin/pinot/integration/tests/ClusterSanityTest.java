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

import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This test sets up the Pinot cluster and ensures the sanity/consistency of
 * cluster after modifications are performed on the cluster.
 */
public class ClusterSanityTest extends ClusterTest {
  private static final String tenantName = ControllerTenantNameBuilder.DEFAULT_TENANT_NAME;
  private static final String brokerTenant = tenantName + "_BROKER";
  private static final String TABLE_NAME = "ClusterSanityTestTable";
  private static final long WAIT_TIME_FOR_TENANT_UPDATE = 2000;

  /**
   * This test ensures that the cluster is in a consistent after the number of brokers
   * is reduced.
   * @throws Exception
   */
  @Test
  public void testBrokerScaleDown()
      throws Exception {
    setupCluster(2, 1);
    scaleDownBroker(1);

    try {
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), brokerTenant).size(), 1);
    } catch (Exception e) {
      Assert.fail("Exception caught while getting all instances in cluster with tag: " + e);
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Set up the Pinot cluster with provided number of brokers and servers.
   *
   * @param numBrokers
   * @param numServers
   * @throws Exception
   */
  public void setupCluster(int numBrokers, int numServers)
      throws Exception {
    startZk();
    startController();
    startBrokers(numBrokers);
    startServers(numServers);
    addOfflineTable("", "", -1, "", tenantName, tenantName, TABLE_NAME, SegmentVersion.v1);
  }

  /**
   * Tear down the Pinot cluster.
   *
   * @throws Exception
   */
  public void tearDownCluster()
      throws Exception {
    stopBroker();
    stopController();
    stopServer();
    stopZk();
  }

  /**
   * Helper method to reduce the number of brokers to the provided value.
   * Assumes that the number of existing brokers is greater then the desired number.
   *
   * @param newNumBrokers
   * @return
   * @throws Exception
   */
  private String scaleDownBroker(int newNumBrokers)
      throws Exception {
    Tenant tenant = new Tenant.TenantBuilder(tenantName).setRole(TenantRole.BROKER)
        .setTotalInstances(newNumBrokers)
        .setOfflineInstances(newNumBrokers)
        .build();

    // Send the 'put' (instead of 'post') request, that updates the tenants instead of creating.
    JSONObject request = tenant.toJSON();
    sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantCreate(),
        request.toString());
    Thread.sleep(WAIT_TIME_FOR_TENANT_UPDATE);

    return brokerTenant;
  }
}
