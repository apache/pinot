package org.apache.pinot.controller;

import java.util.Map;
import org.apache.pinot.controller.api.AccessControlTest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import static org.apache.pinot.controller.ControllerTestUtils.*;


public class ControllerTestSetup {
  @BeforeSuite
  public void suiteSetup() {
    startZk();
    startController(getSuiteControllerConfiguration());
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
    stopController();
    stopZk();
  }
}
