package org.apache.pinot.controller;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_HOST;
import static org.apache.pinot.controller.ControllerConf.CONTROLLER_PORT;
import static org.apache.pinot.controller.ControllerConf.HELIX_CLUSTER_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONTROLLER_DYNAMIC_HELIX_HOST;

public class ControllerStarterTest extends ControllerTest {
  private Map<String, Object> controllerConfigMap = new HashMap<>();
  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> legacy = super.getDefaultControllerConfiguration();
    legacy.keySet().forEach(key -> controllerConfigMap.putIfAbsent(key, legacy.get(key)));
    return controllerConfigMap;
  }

  @Test
  public void testControllerHostNameOverride() {
    boolean controllerStarted = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_DYNAMIC_HELIX_HOST, true);
      controllerConfigMap.put(CONTROLLER_HOST, "strange_name.com");
      controllerConfigMap.put(CONTROLLER_PORT, 9527);
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertEquals("strange_name.com", instanceConfig.getHostName());
      Assert.assertEquals("9527", instanceConfig.getPort());
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  @Test
  public void testSkippingControllerHostNameOverride() {
    boolean controllerStarted = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_DYNAMIC_HELIX_HOST, false);
      controllerConfigMap.put(CONTROLLER_HOST, "strange_name.com");
      controllerConfigMap.put(CONTROLLER_PORT, 9527);
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertNotEquals("strange_name.com", instanceConfig.getHostName());
      Assert.assertNotEquals("9527", instanceConfig.getPort());
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  @Test
  public void testMissingControllerHostName() {
    boolean controllerStarted = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_DYNAMIC_HELIX_HOST, true);
      controllerConfigMap.put(CONTROLLER_HOST, "");
      controllerConfigMap.put(CONTROLLER_PORT, 9527);
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertNotEquals("", instanceConfig.getHostName());
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  @Test
  public void testIntegerPortBlock() {
    boolean controllerStarted = false;
    boolean validated = false;
    boolean numberFormatCaught = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_DYNAMIC_HELIX_HOST, true);
      controllerConfigMap.put(CONTROLLER_HOST, "strange_host.com");
      controllerConfigMap.put(CONTROLLER_PORT, "not-a-number");
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertNotEquals("strange_host.com", instanceConfig.getHostName());
      Assert.assertNotEquals("not-a-number", instanceConfig.getPort());
      validated = true;
    } catch (NumberFormatException ex) {
      numberFormatCaught = true;
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
    Assert.assertTrue(numberFormatCaught || validated);
  }

}
