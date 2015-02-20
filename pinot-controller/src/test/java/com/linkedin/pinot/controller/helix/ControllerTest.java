package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;


/**
 * Base class for controller tests.
 *
 * @author jfim
 */
public abstract class ControllerTest {
  protected static final String ZK_STR = "localhost:2181";
  private static final String CONTROLLER_API_PORT = "8998";
  protected static final String CONTROLLER_BASE_API_URL = StringUtil.join(":", "http://localhost", CONTROLLER_API_PORT);
  private static final String DATA_DIR = "/tmp";
  private static final String CONTROLLER_INSTANCE_NAME = "localhost_11984";
  protected static ZkClient _zkClient = new ZkClient(ZK_STR);
  protected static ControllerStarter _controllerStarter;
  protected HelixAdmin _helixAdmin;
  private HelixManager _helixZkManager;

  /**
   * Starts a controller instance.
   */
  protected void startController() {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost(CONTROLLER_INSTANCE_NAME);
    conf.setControllerPort(CONTROLLER_API_PORT);
    conf.setDataDir(DATA_DIR);
    conf.setZkStr(ZK_STR);
    conf.setHelixClusterName(getHelixClusterName());

    if (ControllerSentinelTest._zkClient.exists("/" + getHelixClusterName())) {
      ControllerSentinelTest._zkClient.deleteRecursive("/" + getHelixClusterName());
    }

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_STR);
    _helixZkManager = HelixSetupUtils.setup(getHelixClusterName(), helixZkURL, CONTROLLER_INSTANCE_NAME);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    ControllerSentinelTest._controllerStarter = new ControllerStarter(conf);
    ControllerSentinelTest._controllerStarter.start();
  }

  protected void stopController() {
    _controllerStarter.stop();
    _zkClient.close();
  }

  protected abstract String getHelixClusterName();
}
