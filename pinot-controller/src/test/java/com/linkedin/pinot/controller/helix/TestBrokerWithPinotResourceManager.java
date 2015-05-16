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

import com.linkedin.pinot.common.ZkTestUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;


public class TestBrokerWithPinotResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestBrokerWithPinotResourceManager.class);

  private PinotHelixResourceManager _pinotResourceManager;
  private final static String ZK_SERVER = ZkTestUtils.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "TestBrokerWithPinotResourceManager";

  private ZkClient _zkClient;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;

  @BeforeTest
  public void setUp() throws Exception {
    ZkTestUtils.startLocalZkServer();
    _zkClient = new ZkClient(ZK_SERVER);
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    Thread.sleep(3000);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, 5);

  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
    ZkTestUtils.stopLocalZkServer();
  }

  @Test
  public void testTagAssignment() throws Exception {

    PinotResourceManagerResponse res;
    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(1,
            "broker_tag0"));
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 4);
    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(2,
            "broker_tag1"));
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag1").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 2);
    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(3,
            "broker_tag2"));
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, false);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag1").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 2);
    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(3,
            "tag2"));
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, false);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag1").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 2);
    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(3,
            "broker_tag1"));
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag1").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 1);
    res = _pinotResourceManager.deleteBrokerResourceTag("broker_tag0");
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_tag1").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 2);
    res = _pinotResourceManager.deleteBrokerResourceTag("broker_tag1");
    System.out.println(res);
    Thread.sleep(2000);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 5);
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    PinotResourceManagerResponse res;
    IdealState idealState;

    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(2,
            "broker_mirror"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 3);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerResourceTag(ControllerRequestBuilderUtil.createBrokerTagResourceConfig(3,
            "broker_colocated"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerDataResource(ControllerRequestBuilderUtil.createBrokerDataResourceConfig(
            "mirror", 2, "broker_mirror"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerDataResource(ControllerRequestBuilderUtil.createBrokerDataResourceConfig(
            "company", 2, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerDataResource(ControllerRequestBuilderUtil.createBrokerDataResourceConfig(
            "scin", 3, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerDataResource(ControllerRequestBuilderUtil.createBrokerDataResourceConfig(
            "cap", 1, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 1);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerDataResource(ControllerRequestBuilderUtil.createBrokerDataResourceConfig(
            "cap", 3, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 3);
    Thread.sleep(2000);

    res =
        _pinotResourceManager.createBrokerDataResource(ControllerRequestBuilderUtil.createBrokerDataResourceConfig(
            "cap", 2, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 2);
    Thread.sleep(2000);

    res = _pinotResourceManager.deleteBrokerDataResource("company");
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 2);
    Thread.sleep(2000);

    res = _pinotResourceManager.deleteBrokerResourceTag("broker_colocated");
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 0);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 3);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 0);
    Thread.sleep(2000);

    res = _pinotResourceManager.deleteBrokerResourceTag("broker_mirror");
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_mirror").size(), 0);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 0);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 5);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 0);
    Thread.sleep(2000);
  }

  public void testWithCmdLines() throws Exception {

    final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      final String command = br.readLine();
      if ((command != null) && command.equals("exit")) {
        tearDown();
      }
    }
  }

}
