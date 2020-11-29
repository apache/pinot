package org.apache.pinot.controller;

import java.util.Map;
import org.apache.pinot.controller.api.AccessControlTest;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import static org.apache.pinot.controller.ControllerTestUtils.*;


public class ControllerTestSetup {
  @BeforeSuite
  public void suiteSetup() throws Exception {
    startZk();
    startController(getSuiteControllerConfiguration());

    // initial
    System.out.println(getHelixAdmin().getInstancesInCluster(getHelixClusterName()));

    addMaxFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKER_INSTANCES, true);
    addMaxFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVER_INSTANCES, true);

    System.out.println(getHelixAdmin().getInstancesInCluster(getHelixClusterName()));

    addMaxFakeBrokerInstancesToAutoJoinHelixCluster(TOTAL_NUM_BROKER_INSTANCES, false);
    addMaxFakeServerInstancesToAutoJoinHelixCluster(TOTAL_NUM_SERVER_INSTANCES, false);

    System.out.println(getHelixAdmin().getInstancesInCluster(getHelixClusterName()));
  }

  public static Map<String, Object> getSuiteControllerConfiguration() {
    Map<String, Object> properties = getDefaultControllerConfiguration();

    // Used in AccessControlTest
    properties.put(ControllerConf.ACCESS_CONTROL_FACTORY_CLASS, AccessControlTest.DenyAllAccessFactory.class.getName());

    // AKL_TODO
    // Used in PinotInstanceAssignmentRestletResourceTest.
    //properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);

    // Used in PinotTableRestletResourceTest
    properties.put(ControllerConf.TABLE_MIN_REPLICAS, MIN_NUM_REPLICAS);

    return properties;
  }


  @AfterSuite
  public void tearDownSuite() {
    getHelixResourceManager().deleteBrokerTenantFor(TENANT_NAME);
    getHelixResourceManager().deleteOfflineServerTenantFor(TENANT_NAME);
    getHelixResourceManager().deleteRealtimeServerTenantFor(TENANT_NAME);

    stopFakeInstances();

    stopController();
    stopZk();
  }
}
