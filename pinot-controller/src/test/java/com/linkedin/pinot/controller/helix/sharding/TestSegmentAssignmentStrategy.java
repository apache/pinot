package com.linkedin.pinot.controller.helix.sharding;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;


public class TestSegmentAssignmentStrategy {
  private static Logger LOGGER = LoggerFactory.getLogger(TestSegmentAssignmentStrategy.class);

  private PinotHelixResourceManager _pinotResourceManager;
  private final String _zkServer = "localhost:2181";
  private final String _helixClusterName = "TestSegmentAssignmentStrategyHelix";

  private final ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance = 30;
  private String _resourceNameBalanced = "testResourceBalanced";
  private String _resourceNameRandom = "testResourceRandom";

  private static String UNTAGGED = "untagged";

  @BeforeTest
  public void setup() throws Exception {
    String zkPath = "/" + HelixConfig.HELIX_ZK_PATH_PREFIX;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(_zkServer, _helixClusterName, instanceId);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkServer);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    //

    addInstancesToAutoJoinHelixCluster(_numInstance);
    Thread.sleep(3000);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
  }

  public static DataResource createOfflineClusterConfig(int numInstancesPerReplica, int numReplicas,
      String resourceName, String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("tableName", "testTable");
    props.put("timeColumnName", "days");
    props.put("timeType", "daysSinceEpoch");
    props.put("numInstances", String.valueOf(numInstancesPerReplica));
    props.put("numReplicas", String.valueOf(numReplicas));
    props.put("retentionTimeUnit", "DAYS");
    props.put("retentionTimeValue", "30");
    props.put("pushFrequency", "daily");
    props.put("segmentAssignmentStrategy", segmentAssignmentStrategy);

    final DataResource res = DataResource.fromMap(props);

    return res;
  }

  private void addInstancesToAutoJoinHelixCluster(int numInstance) throws Exception {
    for (int i = 0; i < numInstance; ++i) {
      final String instanceId = "localhost_" + i;

      _helixZkManager =
          HelixManagerFactory.getZKHelixManager(_helixClusterName, instanceId, InstanceType.PARTICIPANT, _zkServer
              + "/pinot-helix");
      final StateMachineEngine stateMachineEngine = _helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new SegmentOnlineOfflineStateModelFactory(null);
      stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      _helixZkManager.connect();
      _helixZkManager.getClusterManagmentTool().addInstanceTag(_helixClusterName, instanceId, UNTAGGED);
    }
  }

  @Test
  public void testRandomSegmentAssignmentStrategy() throws Exception {
    int numRelicas = 2;
    int numInstancesPerReplica = 10;
    DataResource resource =
        createOfflineClusterConfig(numInstancesPerReplica, numRelicas, _resourceNameRandom, "RandomAssignmentStrategy");
    _pinotResourceManager.createDataResource(resource);
    Thread.sleep(3000);
    for (int i = 0; i < 10; ++i) {
      addOneSegment(_resourceNameRandom);
      Thread.sleep(2000);
      List<String> taggedInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, _resourceNameRandom);
      Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
      }
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, _resourceNameRandom);
      Assert.assertEquals(externalView.getPartitionSet().size(), i + 1);
      for (String segmentId : externalView.getPartitionSet()) {
        Assert.assertEquals(externalView.getStateMap(segmentId).size(), numRelicas);
      }

    }
  }

  public void testBucketizedSegmentAssignmentStrategy() {

  }

  @Test
  public void testBalanceNumSegmentAssignmentStrategy() throws Exception {
    int numRelicas = 3;
    int numInstancesPerReplica = 2;
    DataResource resource =
        createOfflineClusterConfig(numInstancesPerReplica, numRelicas, _resourceNameBalanced,
            "BalanceNumSegmentAssignmentStrategy");
    _pinotResourceManager.createDataResource(resource);
    Thread.sleep(3000);
    for (int i = 0; i < 10; ++i) {
      addOneSegment(_resourceNameBalanced);
      Thread.sleep(2000);
      List<String> taggedInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, _resourceNameBalanced);
      Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
      }
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, _resourceNameBalanced);
      for (String segmentId : externalView.getPartitionSet()) {
        for (String instance : externalView.getStateMap(segmentId).keySet()) {
          instance2NumSegmentsMap.put(instance, instance2NumSegmentsMap.get(instance) + 1);
        }
      }
      int totalSegments = (i + 1) * numRelicas;
      int totalInstances = numInstancesPerReplica * numRelicas;
      int minNumSegmentsPerInstance = totalSegments / totalInstances;
      int maxNumSegmentsPerInstance = minNumSegmentsPerInstance;
      if ((minNumSegmentsPerInstance * totalInstances) < totalSegments) {
        maxNumSegmentsPerInstance = maxNumSegmentsPerInstance + 1;
      }
      for (String instance : instance2NumSegmentsMap.keySet()) {
        Assert.assertTrue(instance2NumSegmentsMap.get(instance) >= minNumSegmentsPerInstance);
        Assert.assertTrue(instance2NumSegmentsMap.get(instance) <= maxNumSegmentsPerInstance);
      }
    }

    _helixAdmin.dropResource(_helixClusterName, _resourceNameBalanced);
  }

  private void addOneSegment(String resourceName) {
    SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addSegment(segmentMetadata);
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotResourceManager.deleteSegment(resource, segment);
  }
}
