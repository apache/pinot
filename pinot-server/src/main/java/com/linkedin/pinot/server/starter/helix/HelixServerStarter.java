package com.linkedin.pinot.server.starter.helix;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;


/**
 * Single server helix starter. Will start automatically with an untagged box.
 * Will auto join current cluster as a participant.
 *
 *
 * @author xiafu
 *
 */
public class HelixServerStarter {

  private final HelixManager _helixManager;
  private final Configuration _pinotHelixProperties;

  private static ServerConf _serverConf;
  private static ServerInstance _serverInstance;
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {

    _pinotHelixProperties = pinotHelixProperties;
    final String instanceId = pinotHelixProperties.getString("instanceId", "dataServer_" + NetUtil.getHostAddress());

    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    startServerInstance(pinotHelixProperties);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory = new SegmentOnlineOfflineStateModelFactory();
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixManager.connect();
    _helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
        V1Constants.Helix.UNTAGGED_SERVER_INSTANCE);
  }

  private void startServerInstance(Configuration moreConfigurations) throws Exception {
    _serverConf = getInstanceServerConfig(moreConfigurations);
    if (_serverInstance == null) {
      LOGGER.info("Trying to create a new ServerInstance!");
      _serverInstance = new ServerInstance();
      LOGGER.info("Trying to initial ServerInstance!");
      _serverInstance.init(_serverConf);
      LOGGER.info("Trying to start ServerInstance!");
      _serverInstance.start();
    }
  }

  private ServerConf getInstanceServerConfig(Configuration moreConfigurations) {
    return DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(moreConfigurations);
  }

}
