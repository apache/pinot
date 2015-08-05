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
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;


public class ControllerTenantTest extends ControllerTest {

  private static final String HELIX_CLUSTER_NAME = "ControllerTenantTest";
  static ZkClient _zkClient = null;

  @BeforeClass
  public void setup() throws Exception {
    startZk();
    _zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    startController();
    ZKMetadataProvider.setClusterTenantIsolationEnabled(_propertyStore, false);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 20);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 20);

  }

  @AfterClass
  public void tearDown() {
    stopController();
    try {
      if (_zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
        _zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
      }
    } catch (Exception e) {
    }
    _zkClient.close();
    stopZk();
  }

  @Test
  public void testBrokerTenantCreation() throws JSONException, UnsupportedEncodingException, IOException {
    for (int i = 0; i < 4; i++) {
      String brokerTag = "colocated_" + i;
      final JSONObject payload = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(brokerTag, 5);
      final String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
              payload.toString());
      System.out.println(res);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size());
      Assert
          .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), 5);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size());
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), 15 - i * 5);
    }
    for (int i = 0; i < 4; i++) {
      String brokerTag = "colocated_" + i;
      final String res =
          sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantDelete(
              brokerTag));
      System.out.println(res);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size());
      Assert
          .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), 0);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), i * 5 + 5);
    }
  }

  @Test
  public void testBrokerTenantUpdate() throws JSONException, UnsupportedEncodingException, IOException {
    String brokerTag = "colocated_0";
    JSONObject payload = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(brokerTag, 5);
    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
            payload.toString());
    System.out.println(res);
    System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size());
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), 5);
    System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
        CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size());
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
            .size(), 15);

    for (int i = 6; i < 15; ++i) {
      payload = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(brokerTag, i);
      res =
          sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
              payload.toString());
      Assert
          .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), i);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), 20 - i);
    }
    res =
        sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantDelete(brokerTag));
    System.out.println(res);
    System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
        CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size());
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), 0);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
            .size(), 20);
  }

  @Test
  public void testServerTenantCreation() throws JSONException, UnsupportedEncodingException, IOException {
    for (int i = 0; i < 4; i++) {
      String serverTag = "serverTag_" + i;
      final JSONObject payload = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(serverTag, 5, 2, 3);
      final String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
              payload.toString());
      System.out.println(res);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(),
          2);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(),
          3);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), 20 - 5 * (i + 1));
    }

    for (int i = 0; i < 4; i++) {
      String serverTag = "serverTag_" + i;
      final String res =
          sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forServerTenantDelete(
              serverTag));
      System.out.println(res);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE).size());
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(),
          0);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(),
          0);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), i * 5 + 5);
    }
  }

  @Test
  public void testServerTenantUpdate() throws JSONException, UnsupportedEncodingException, IOException {
    String serverTag = "serverTag_0";
    JSONObject payload = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(serverTag, 5, 2, 3);
    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
            payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(), 2);
    Assert
        .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(), 3);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            .size(), 15);

    for (int i = 6; i < 18; i += 2) {
      payload = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(serverTag, i, i / 2, i / 2);
      res =
          sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
              payload.toString());
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(),
          i / 2);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(),
          i / 2);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), 20 - i);
    }
    res =
        sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forServerTenantDelete(serverTag));
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(), 0);
    Assert
        .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(), 0);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            .size(), 20);
  }

  @Test
  public void testGetBrokerCall() throws JSONException, UnsupportedEncodingException, IOException {
    for (int i = 0; i < 4; i++) {
      String brokerTag = "colocated_" + i;
      JSONObject payload = ControllerRequestBuilderUtil.buildBrokerTenantCreateRequestJSON(brokerTag, 5);
      String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
              payload.toString());
      System.out.println(res);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size());
      Assert
          .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), 5);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size());
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), 15 - i * 5);
    }
    String res = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantGet());
    System.out.println("**************");
    System.out.println(res);
    JSONObject resJsonObject = new JSONObject(res);
    Assert.assertEquals(resJsonObject.getJSONArray("BROKER_TENANTS").length(), 4);
    for (int i = 0; i < 4; i++) {
      String brokerTag = "colocated_" + i;
      res = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantGet(brokerTag));
      System.out.println("**************");
      System.out.println(res);
      resJsonObject = new JSONObject(res);
      Assert.assertEquals(resJsonObject.getJSONArray("BrokerInstances").length(), 5);
      Assert.assertEquals(resJsonObject.getString("tenantName"), brokerTag);
      res = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantGet(brokerTag));
      System.out.println("**************");
      System.out.println(res);
      resJsonObject = new JSONObject(res);
      Assert.assertEquals(resJsonObject.getJSONArray("BrokerInstances").length(), 5);
      Assert.assertEquals(resJsonObject.getJSONArray("ServerInstances").length(), 0);
      Assert.assertEquals(resJsonObject.getString("tenantName"), brokerTag);
    }
    for (int i = 0; i < 4; i++) {
      String brokerTag = "colocated_" + i;
      res =
          sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forBrokerTenantDelete(
              brokerTag));
      System.out.println(res);
      System.out.println(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE).size());
      Assert
          .assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, brokerTag + "_BROKER").size(), 0);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE)
              .size(), i * 5 + 5);
    }
  }

  @Test
  public void testGetServerCall() throws JSONException, UnsupportedEncodingException, IOException {
    for (int i = 0; i < 4; i++) {
      String serverTag = "serverTag_" + i;
      final JSONObject payload = ControllerRequestBuilderUtil.buildServerTenantCreateRequestJSON(serverTag, 5, 2, 3);
      final String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantCreate(),
              payload.toString());
      System.out.println(res);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(),
          2);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(),
          3);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), 20 - 5 * (i + 1));
    }
    String res = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantGet());
    System.out.println("**************");
    System.out.println(res);
    JSONObject resJsonObject = new JSONObject(res);
    Assert.assertEquals(resJsonObject.getJSONArray("SERVER_TENANTS").length(), 8);
    for (int i = 0; i < 4; i++) {
      String serverTag = "serverTag_" + i;
      res = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forServerTenantGet(serverTag));
      System.out.println("**************");
      System.out.println(res);
      resJsonObject = new JSONObject(res);
      Assert.assertEquals(resJsonObject.getJSONArray("ServerInstances").length(), 5);
      Assert.assertEquals(resJsonObject.getString("tenantName"), serverTag);
      res = sendGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTenantGet(serverTag));
      System.out.println("**************");
      System.out.println(res);
      resJsonObject = new JSONObject(res);
      Assert.assertEquals(resJsonObject.getJSONArray("BrokerInstances").length(), 0);
      Assert.assertEquals(resJsonObject.getJSONArray("ServerInstances").length(), 5);
      Assert.assertEquals(resJsonObject.getString("tenantName"), serverTag);
    }
    for (int i = 0; i < 4; i++) {
      String serverTag = "serverTag_" + i;
      res =
          sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forServerTenantDelete(
              serverTag));
      System.out.println(res);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_OFFLINE").size(),
          0);
      Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, serverTag + "_REALTIME").size(),
          0);
      Assert.assertEquals(
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
              .size(), i * 5 + 5);
    }
  }

  @Override
  protected String getHelixClusterName() {
    return HELIX_CLUSTER_NAME;
  }
}
