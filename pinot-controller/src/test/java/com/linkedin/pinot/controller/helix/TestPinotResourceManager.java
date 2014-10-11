package com.linkedin.pinot.controller.helix;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.PropertiesConfiguration;
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
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import com.linkedin.pinot.server.starter.helix.PinotHelixStarter;


public class TestPinotResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(TestPinotResourceManager.class);

  private PinotHelixResourceManager _pinotResourceManager;
  private final String _zkServer = "localhost:2181";
  private final String _helixClusterName = "TestPinotResourceManager";

  private final ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;
  private final String _resourceName = "testResource";

  private static String UNTAGGED = "untagged";

  @BeforeTest
  private void setUp() throws Exception {
    final String zkPath = "/" + HelixConfig.HELIX_ZK_PATH_PREFIX + "/" + _helixClusterName;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(_zkServer, _helixClusterName, instanceId);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkServer);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    /////////////////////////
    _numInstance = 1;
    final List<PinotHelixStarter> pinotHelixStarters = addInstancesToAutoJoinHelixCluster(_numInstance);
    Thread.sleep(3000);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    final DataResource resource = createOfflineClusterConfig(1, 1, _resourceName, "BalanceNumSegmentAssignmentStrategy");

    _pinotResourceManager.createDataResource(resource);

  }

  private List<PinotHelixStarter> addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    final List<PinotHelixStarter> pinotHelixStarters = new ArrayList<PinotHelixStarter>();
    for (int i = 0; i < numInstances; ++i) {
      final PinotHelixStarter pinotHelixStarter =
          new PinotHelixStarter(_helixClusterName, _zkServer, new PropertiesConfiguration());
      pinotHelixStarters.add(pinotHelixStarter);
      Thread.sleep(1000);
    }
    return pinotHelixStarters;
  }

  @AfterTest
  private void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
  }

  @Test
  public void testAddingAndDeletingSegments() throws Exception {
    for (int i = 1; i <= 5; i++) {
      addOneSegment(_resourceName);
      Thread.sleep(2000);
      final ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, _resourceName);
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
    final ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, _resourceName);
    int i = 4;
    for (final String segmentId : externalView.getPartitionSet()) {
      deleteOneSegment(_resourceName, segmentId);
      Thread.sleep(2000);
      Assert.assertEquals(_helixAdmin.getResourceExternalView(_helixClusterName, _resourceName).getPartitionSet()
          .size(), i);
      i--;
    }
  }

  public void testWithCmdLines() throws Exception {

    final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      final String command = br.readLine();
      if ((command != null) && command.equals("exit")) {
        tearDown();
      }
      if ((command != null) && command.equals("add")) {
        addOneSegment(_resourceName);
      }
      if ((command != null) && command.startsWith("delete")) {
        final String segment2delete = command.split(" ")[1];
        deleteOneSegment(_resourceName, segment2delete);
      }
    }
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(_resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addSegment(segmentMetadata, "downloadUrl");
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotResourceManager.deleteSegment(resource, segment);
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

}
