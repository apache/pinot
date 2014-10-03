package com.linkedin.pinot.controller.helix;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestPinotResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(TestPinotResourceManager.class);
  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestPinotResourceManager.class.toString());

  private PinotHelixResourceManager _pinotResourceManager;
  private final String _zkServer = "localhost:2181";
  private final String _helixClusterName = "pinotClusterOne";

  private final ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;

  private static String UNTAGGED = "untagged";

  @BeforeTest
  private void setUp() throws Exception {

    if (_zkClient.exists("/pinot-helix")) {
      _zkClient.deleteRecursive("/pinot-helix");
    }
    final String instanceId = "localhost:helixController";
    _pinotResourceManager = new PinotHelixResourceManager(_zkServer, _helixClusterName, instanceId);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkServer);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
  }

  private List<Object> addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    //    final List<PinotHelixStarter> pinotHelixStarters = new ArrayList<PinotHelixStarter>();
    //    for (int i = 0; i < numInstances; ++i) {
    //      final PropertiesConfiguration config = new PropertiesConfiguration();
    //      config.addProperty("instanceId", "localhost_100" + i);
    //
    //      final PinotHelixStarter pinotHelixStarter =
    //
    //          new PinotHelixStarter(_helixClusterName, _zkServer, config);
    //      pinotHelixStarters.add(pinotHelixStarter);
    //      Thread.sleep(1000);
    //    }
    //  return pinotHelixStarters;
    return null;
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
    //final List<PinotHelixStarter> pinotHelixStarters = addInstancesToAutoJoinHelixCluster(_numInstance);
    Thread.sleep(3000);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    final String resourceName = "testCreateNewConfigForExistedResource";
    final DataResource resource = createOfflineClusterConfig(2, 3, resourceName);

    _pinotResourceManager.createDataResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getResourceName()).size(), 6);
    _pinotResourceManager.startInstances(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getResourceName()));
    Thread.sleep(3000);
    IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(1000, resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + indexSegment.getSegmentName());
    _pinotResourceManager.addSegment(indexSegment);
    indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(1000, resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + indexSegment.getSegmentName());
    _pinotResourceManager.addSegment(indexSegment);
    Thread.sleep(3000);
  }

  public static DataResource createOfflineClusterConfig(int numInstancesPerReplica, int numReplicas, String resourceName) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("tableName", resourceName);
    props.put("timeColumnName", "days");
    props.put("timeType", "daysSinceEpoch");
    props.put("numInstances", String.valueOf(numInstancesPerReplica));
    props.put("numReplicas", String.valueOf(numReplicas));
    props.put("retentionTimeUnit", "DAYS");
    props.put("retentionTimeValue", "30");
    props.put("pushFrequency", "daily");

    final DataResource res = DataResource.fromMap(props);

    return res;
  }

}
