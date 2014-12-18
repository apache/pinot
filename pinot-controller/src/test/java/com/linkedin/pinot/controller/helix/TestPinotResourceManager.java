package com.linkedin.pinot.controller.helix;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;


public class TestPinotResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(TestPinotResourceManager.class);

  private PinotHelixResourceManager _pinotResourceManager;
  private final static String ZK_SERVER = "localhost:2181";
  private final static String HELIX_CLUSTER_NAME = "TestPinotResourceManager";
  private final static String TEST_RESOURCE_NAME = "testResource";

  private final ZkClient _zkClient = new ZkClient(ZK_SERVER);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;

  @BeforeTest
  public void setUp() throws Exception {
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

    /////////////////////////
    _numInstance = 1;
    final List<HelixServerStarter> pinotHelixStarters = addInstancesToAutoJoinHelixCluster(_numInstance);
    Thread.sleep(3000);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            .size(), _numInstance);
    final DataResource resource =
        ControllerRequestBuilderUtil.createOfflineClusterCreationConfig(1, 1, TEST_RESOURCE_NAME,
            "BalanceNumSegmentAssignmentStrategy");

    _pinotResourceManager.createNewDataResource(resource);

  }

  private List<HelixServerStarter> addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    final List<HelixServerStarter> pinotHelixStarters = new ArrayList<HelixServerStarter>();
    for (int i = 0; i < numInstances; ++i) {
      final HelixServerStarter pinotHelixStarter =
          new HelixServerStarter(HELIX_CLUSTER_NAME, ZK_SERVER, new PropertiesConfiguration());
      pinotHelixStarters.add(pinotHelixStarter);
      Thread.sleep(1000);
    }
    return pinotHelixStarters;
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
  }

  @Test
  public void testAddingAndDeletingSegments() throws Exception {
    for (int i = 1; i <= 5; i++) {
      addOneSegment(TEST_RESOURCE_NAME);
      Thread.sleep(2000);
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TEST_RESOURCE_NAME);
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
    final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TEST_RESOURCE_NAME);
    int i = 4;
    for (final String segmentId : externalView.getPartitionSet()) {
      deleteOneSegment(TEST_RESOURCE_NAME, segmentId);
      Thread.sleep(2000);
      Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TEST_RESOURCE_NAME).getPartitionSet()
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
        addOneSegment(TEST_RESOURCE_NAME);
      }
      if ((command != null) && command.startsWith("delete")) {
        final String segment2delete = command.split(" ")[1];
        deleteOneSegment(TEST_RESOURCE_NAME, segment2delete);
      }
    }
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(TEST_RESOURCE_NAME, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addSegment(segmentMetadata, "downloadUrl");
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotResourceManager.deleteSegment(resource, segment);
  }

}
