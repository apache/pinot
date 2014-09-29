package com.linkedin.pinot.controller.helix;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.IncomingConfigParamKeys;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.properties.PinotResourceCreationRequestBuilder;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.starter.helix.PinotHelixStarter;


public class TestPinotResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(TestPinotResourceManager.class);
  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestPinotResourceManager.class.toString());

  private PinotHelixResourceManager _pinotResourceManager;
  private String _zkServer = "localhost:2181";
  private String _helixClusterName = "pinotClusterOne";

  private ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;

  private static String UNTAGGED = "untagged";

  @BeforeTest
  private void setUp() throws Exception {

    if (_zkClient.exists("/pinot-helix")) {
      _zkClient.deleteRecursive("/pinot-helix");
    }
    String instanceId = "localhost:helixController";
    _pinotResourceManager = new PinotHelixResourceManager(_zkServer, _helixClusterName, instanceId);
    _pinotResourceManager.start();

    String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkServer);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
  }

  private List<PinotHelixStarter> addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    List<PinotHelixStarter> pinotHelixStarters = new ArrayList<PinotHelixStarter>();
    for (int i = 0; i < numInstances; ++i) {
      PinotHelixStarter pinotHelixStarter =
          new PinotHelixStarter(_helixClusterName, _zkServer, new PropertiesConfiguration());
      pinotHelixStarters.add(pinotHelixStarter);
      Thread.sleep(1000);
    }
    return pinotHelixStarters;
  }

  @AfterTest
  private void tearDown() {
    _pinotResourceManager.stop();
    if (_zkClient.exists("/pinot-helix")) {
      _zkClient.deleteRecursive("/pinot-helix");
    }
  }

  @Test
  public void testAddingSegments() throws Exception {
    _numInstance = 6;
    List<PinotHelixStarter> pinotHelixStarters = addInstancesToAutoJoinHelixCluster(_numInstance);
    Thread.sleep(3000);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewConfigForExistedResource";
    Map<String, String> serverConfig = createOfflineClusterConfig(2, 3, resourceName);
    serverConfig.put("d2ClusterName", "testCreateNewConfigForExistedResourceD2");

    PinotStandaloneResource resource = PinotResourceCreationRequestBuilder.buildOfflineResource(serverConfig);

    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);
    _pinotResourceManager
        .startInstances(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()));
    Thread.sleep(3000);
    IndexSegment indexSegment =
        IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(1000, resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + indexSegment.getSegmentName());
    _pinotResourceManager.addSegment(indexSegment);
    indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(1000, resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + indexSegment.getSegmentName());
    _pinotResourceManager.addSegment(indexSegment);
    Thread.sleep(3000);
    //    _pinotResourceManager.deleteResource(resource.getTag());
    //    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    //    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 0);
  }

  public static Map<String, String> createOfflineClusterConfig(int numInstancesPerReplica, int numReplicas,
      String resourceName) {
    Map<String, String> serverConfig = new HashMap<String, String>();
    serverConfig.put(IncomingConfigParamKeys.Offline.offlineResourceName.toString(), resourceName);
    serverConfig.put(IncomingConfigParamKeys.Offline.offlineNumInstancesPerReplica.toString(),
        String.valueOf(numInstancesPerReplica));
    serverConfig.put(IncomingConfigParamKeys.Offline.offlineNumReplicas.toString(), String.valueOf(numReplicas));

    serverConfig.put(IncomingConfigParamKeys.Offline.offlineRetentionDuration.toString(), "30");
    serverConfig.put(IncomingConfigParamKeys.Offline.offlineRetentionTimeColumn.toString(), "daysSinceEpoich");
    serverConfig.put(IncomingConfigParamKeys.Offline.offlineRetentionTimeUnit.toString(), "DAYS ");

    return serverConfig;
  }

}
