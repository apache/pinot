package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import java.io.File;
import org.apache.commons.io.FileUtils;


/**
 * Utilities to start controllers during unit tests.
 *
 * @author jfim
 */
public class ControllerTestUtils {
  public static final String DEFAULT_CONTROLLER_INSTANCE_NAME = "localhost_11984";
  public static final String DEFAULT_DATA_DIR = FileUtils.getTempDirectoryPath() + File.separator + "test-controller-" + System.currentTimeMillis();
  public static final String DEFAULT_CONTROLLER_API_PORT = "8998";

  public static ControllerConf getDefaultControllerConfiguration() {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost(DEFAULT_CONTROLLER_INSTANCE_NAME);
    conf.setControllerPort(DEFAULT_CONTROLLER_API_PORT);
    conf.setDataDir(DEFAULT_DATA_DIR);
    conf.setControllerVipHost("localhost");

    return conf;
  }

  public static ControllerStarter startController(final String clusterName, final String zkStr, final ControllerConf configuration) {
    configuration.setHelixClusterName(clusterName);
    configuration.setZkStr(zkStr);

    ControllerStarter controllerStarter = new ControllerStarter(configuration);
    controllerStarter.start();
    return controllerStarter;
  }

  public static void stopController(final ControllerStarter controllerStarter) {
    controllerStarter.stop();
  }
}
