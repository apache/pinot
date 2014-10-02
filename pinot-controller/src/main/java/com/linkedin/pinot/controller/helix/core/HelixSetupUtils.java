package com.linkedin.pinot.controller.helix.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;


public class HelixSetupUtils {

  private static final Logger logger = Logger.getLogger(HelixSetupUtils.class);

  public static synchronized HelixManager setup(String helixClusterName, String zkPath, String pinotControllerInstanceId) {

    try {
      createHelixClusterIfNeeded(helixClusterName, zkPath);
    } catch (final Exception e) {
      logger.error(e);
      return null;
    }

    try {
      return startHelixControllerInStandadloneMode(helixClusterName, zkPath, pinotControllerInstanceId);
    } catch (final Exception e) {
      logger.error(e);
      return null;
    }
  }

  private static void createHelixClusterIfNeeded(String helixClusterName, String zkPath) {
    final HelixAdmin admin = new ZKHelixAdmin(zkPath);

    if (admin.getClusters().contains(helixClusterName)) {
      logger.info("cluster already exist, skipping it.. ********************************************* ");
      return;
    }

    logger.info("Creating a new cluster, as the helix cluster : " + helixClusterName + " was not found ********************************************* ");
    admin.addCluster(helixClusterName, false);

    logger.info("Enable auto join.");
    final HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(helixClusterName).build();

    final Map<String, String> props = new HashMap<String, String>();
    props.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));

    admin.setConfig(scope, props);

    logger.info("Adding state model definition named : "
        + PinotHelixStateModelGenerator.PINOT_HELIX_STATE_MODEL + " generated using : "
        + PinotHelixStateModelGenerator.class.toString() + " ********************************************** ");

    admin.addStateModelDef(helixClusterName, PinotHelixStateModelGenerator.PINOT_HELIX_STATE_MODEL,
        PinotHelixStateModelGenerator.generatePinotStateModelDefinition());

    logger.info("New Cluster setup completed... ********************************************** ");
  }

  private static HelixManager startHelixControllerInStandadloneMode(String helixClusterName, String zkUrl,
      String pinotControllerInstanceId) {
    logger.info("Starting Helix Standalone Controller ... ");
    return HelixControllerMain.startHelixController(zkUrl, helixClusterName, pinotControllerInstanceId,
        HelixControllerMain.STANDALONE);
  }
}
