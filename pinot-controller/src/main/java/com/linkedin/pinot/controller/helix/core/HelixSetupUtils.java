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
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.utils.CommonConstants;


/**
 * HelixSetupUtils handles how to create or get a helixCluster in controller.
 * 
 * @author xiafu
 *
 */
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

    logger.info("Creating a new cluster, as the helix cluster : " + helixClusterName
        + " was not found ********************************************* ");
    admin.addCluster(helixClusterName, false);

    logger.info("Enable auto join.");
    final HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(helixClusterName).build();

    final Map<String, String> props = new HashMap<String, String>();
    props.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));

    admin.setConfig(scope, props);

    logger.info("Adding state model definition named : "
        + PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL
        + " generated using : " + PinotHelixSegmentOnlineOfflineStateModelGenerator.class.toString()
        + " ********************************************** ");

    admin.addStateModelDef(helixClusterName,
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL,
        PinotHelixSegmentOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());

    logger.info("Adding state model definition named : "
        + PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL
        + " generated using : " + PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.class.toString()
        + " ********************************************** ");

    admin.addStateModelDef(helixClusterName,
        PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL,
        PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());

    logger.info("Adding empty ideal state for Broker!");
    HelixHelper.updateResourceConfigsFor(new HashMap<String, String>(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        helixClusterName, admin);
    IdealState idealState =
        PinotResourceIdealStateBuilder.buildEmptyIdealStateForBrokerResource(admin, helixClusterName);
    admin.setResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, idealState);
    logger.info("New Cluster setup completed... ********************************************** ");
  }

  private static HelixManager startHelixControllerInStandadloneMode(String helixClusterName, String zkUrl,
      String pinotControllerInstanceId) {
    logger.info("Starting Helix Standalone Controller ... ");
    return HelixControllerMain.startHelixController(zkUrl, helixClusterName, pinotControllerInstanceId,
        HelixControllerMain.STANDALONE);
  }
}
