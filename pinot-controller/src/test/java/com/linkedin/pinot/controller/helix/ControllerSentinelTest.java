/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.HelixHelper;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class ControllerSentinelTest extends ControllerTest {
  private static final String FAILURE_STATUS = "failure";
  private static final String SUCCESS_STATUS = "success";

  private static final Logger logger = Logger.getLogger(ControllerSentinelTest.class);

  private static final String HELIX_CLUSTER_NAME = "ControllerSentinelTest";
  static final ZkClient _zkClient = new ZkClient("localhost:2181");

  @BeforeClass
  public void setup() throws Exception {
    startController();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 20);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 20);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    try {
      if (_zkClient.exists("/ControllerSentinelTest")) {
        _zkClient.deleteRecursive("/ControllerSentinelTest");
      }
    } catch (Exception e) {
    }
    _zkClient.close();
  }

  @Test
  public void testAddAlreadyAddedInstance() throws JSONException, UnsupportedEncodingException, IOException {
    for (int i = 0; i < 20; i++) {
      final JSONObject payload =
          ControllerRequestBuilderUtil.buildInstanceCreateRequestJSON("localhost", String.valueOf(i),
              CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      final String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceCreate(),
              payload.toString());
      final JSONObject resJSON = new JSONObject(res);
      Assert.assertEquals(SUCCESS_STATUS, resJSON.getString("status"));
    }
  }

  @Test
  public void testCreateResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("mirror", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
    Assert.assertEquals(SUCCESS_STATUS, new JSONObject(res).getString("status"));
    System.out.println(res);
  }

  @Test
  public void testUpdateResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testUpdateResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
  }

  @Test
  public void testClusterExpansionResource() throws JSONException, UnsupportedEncodingException, IOException, InterruptedException {
    String tag = "testExpansionResource";
    JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON(tag, 2, 2);
    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 1);
    Thread.sleep(1000);
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, "testExpansionResource_O");
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataReplicas(), 2);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataInstances(), 2);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumBrokerInstance(), 1);

    // Update DataResource
    payload = ControllerRequestBuilderUtil.buildUpdateDataResourceJSON("testExpansionResource", 4, 3);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 4);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 1);
    Thread.sleep(1000);
    offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, "testExpansionResource_O");
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataReplicas(), 3);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataInstances(), 4);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumBrokerInstance(), 1);

    //Update DataResource
    payload = ControllerRequestBuilderUtil.buildUpdateDataResourceJSON("testExpansionResource", 6, 5);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 1);
    Thread.sleep(1000);
    offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, "testExpansionResource_O");
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataReplicas(), 5);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataInstances(), 6);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumBrokerInstance(), 1);

    // Update BrokerResource
    payload = ControllerRequestBuilderUtil.buildUpdateBrokerResourceJSON("testExpansionResource", 2);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 2);
    Thread.sleep(1000);
    offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, "testExpansionResource_O");
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataReplicas(), 5);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataInstances(), 6);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumBrokerInstance(), 2);

    // Update BrokerResource
    payload = ControllerRequestBuilderUtil.buildUpdateBrokerResourceJSON("testExpansionResource", 4);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 4);
    Thread.sleep(1000);
    offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, "testExpansionResource_O");
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataReplicas(), 5);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumDataInstances(), 6);
    Assert.assertEquals(offlineDataResourceZKMetadata.getNumBrokerInstance(), 4);
  }

  @Test
  public void testDeleteResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testDeleteResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    Assert.assertEquals(SUCCESS_STATUS, new JSONObject(res).getString("status"));
    final String deleteRes =
        sendDeleteRequest(
            ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceDelete("testDeleteResource_O"));
    final JSONObject resJSON = new JSONObject(deleteRes);
    Assert.assertEquals(SUCCESS_STATUS, resJSON.getString("status"));
  }

  @Test
  public void testGetResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testGetResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println("**************");
    System.out.println(res);
    System.out.println("**************");
    final String getResponse =
        sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceGet("testGetResource"));
    System.out.println("**************");
    System.out.println(getResponse);
    System.out.println("**************");
    final JSONObject getResJSON = new JSONObject(getResponse);
    Assert.assertEquals("testGetResource", getResJSON.getString(CommonConstants.Helix.DataSource.RESOURCE_NAME));
    final String getAllResponse =
        sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceGet(null));
    System.out.println("**************");
    System.out.println(getAllResponse);
    System.out.println("**************");
    Assert.assertEquals(getAllResponse.contains("testGetResource"), true);
    Assert.assertEquals(getAllResponse.contains(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE), true);
  }

  @Override
  protected String getHelixClusterName() {
    return HELIX_CLUSTER_NAME;
  }
}
