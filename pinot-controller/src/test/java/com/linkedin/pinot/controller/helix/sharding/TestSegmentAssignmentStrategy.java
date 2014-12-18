package com.linkedin.pinot.controller.helix.sharding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;


public class TestSegmentAssignmentStrategy {
  private static Logger LOGGER = LoggerFactory.getLogger(TestSegmentAssignmentStrategy.class);

  private final static String ZK_SERVER = "localhost:2181";
  private final static String HELIX_CLUSTER_NAME = "TestSegmentAssignmentStrategyHelix";
  private final static String RESOURCE_NAME_BALANCED = "testResourceBalanced";
  private final static String rESOURCE_NAME_RANDOM = "testResourceRandom";

  private PinotHelixResourceManager _pinotResourceManager;
  private final ZkClient _zkClient = new ZkClient(ZK_SERVER);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private final int _numServerInstance = 30;
  private final int _numBrokerInstance = 5;

  @BeforeTest
  public void setup() throws Exception {
    final String zkPath = "/" + HELIX_CLUSTER_NAME;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    //

    ControllerRequestBuilderUtil
        .addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, _numServerInstance);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER,
        _numBrokerInstance);
    Thread.sleep(3000);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            .size(), _numServerInstance);
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
  }

  @Test
  public void testRandomSegmentAssignmentStrategy() throws Exception {
    final int numRelicas = 2;
    final int numInstancesPerReplica = 10;
    final int totalNumInstances = numRelicas * numInstancesPerReplica;
    final DataResource resource =
        ControllerRequestBuilderUtil.createOfflineClusterCreationConfig(totalNumInstances, numRelicas,
            rESOURCE_NAME_RANDOM,
            "RandomAssignmentStrategy");
    _pinotResourceManager.handleCreateNewDataResource(resource);
    Thread.sleep(3000);
    for (int i = 0; i < 10; ++i) {
      addOneSegment(rESOURCE_NAME_RANDOM);
      Thread.sleep(2000);
      final List<String> taggedInstances =
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, rESOURCE_NAME_RANDOM);
      final Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (final String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
      }
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, rESOURCE_NAME_RANDOM);
      Assert.assertEquals(externalView.getPartitionSet().size(), i + 1);
      for (final String segmentId : externalView.getPartitionSet()) {
        Assert.assertEquals(externalView.getStateMap(segmentId).size(), numRelicas);
      }

    }
  }

  public void testBucketizedSegmentAssignmentStrategy() {

  }

  @Test
  public void testBalanceNumSegmentAssignmentStrategy() throws Exception {
    final int numRelicas = 3;
    final int numInstancesPerReplica = 2;
    final int totalInstances = numInstancesPerReplica * numRelicas;
    final DataResource resource =
        ControllerRequestBuilderUtil.createOfflineClusterCreationConfig(totalInstances, numRelicas,
            RESOURCE_NAME_BALANCED,
            "BalanceNumSegmentAssignmentStrategy");
    _pinotResourceManager.handleCreateNewDataResource(resource);
    Thread.sleep(3000);
    for (int i = 0; i < 10; ++i) {
      addOneSegment(RESOURCE_NAME_BALANCED);
      Thread.sleep(2000);
      final List<String> taggedInstances =
          _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, RESOURCE_NAME_BALANCED);
      final Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (final String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
      }
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, RESOURCE_NAME_BALANCED);
      for (final String segmentId : externalView.getPartitionSet()) {
        for (final String instance : externalView.getStateMap(segmentId).keySet()) {
          instance2NumSegmentsMap.put(instance, instance2NumSegmentsMap.get(instance) + 1);
        }
      }
      final int totalSegments = (i + 1) * numRelicas;
      final int minNumSegmentsPerInstance = totalSegments / totalInstances;
      int maxNumSegmentsPerInstance = minNumSegmentsPerInstance;
      if ((minNumSegmentsPerInstance * totalInstances) < totalSegments) {
        maxNumSegmentsPerInstance = maxNumSegmentsPerInstance + 1;
      }
      for (final String instance : instance2NumSegmentsMap.keySet()) {
        Assert.assertTrue(instance2NumSegmentsMap.get(instance) >= minNumSegmentsPerInstance);
        Assert.assertTrue(instance2NumSegmentsMap.get(instance) <= maxNumSegmentsPerInstance);
      }
    }

    _helixAdmin.dropResource(HELIX_CLUSTER_NAME, RESOURCE_NAME_BALANCED);
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addSegment(segmentMetadata, "downloadUrl");
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotResourceManager.deleteSegment(resource, segment);
  }
}
