package com.linkedin.pinot.server.starter.helix;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader;
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
    final String instanceId =
        pinotHelixProperties.getString(
            "instanceId",
            CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE
                + NetUtil.getHostAddress()
                + "_"
                + pinotHelixProperties.getInt(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
                    CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT));

    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    startServerInstance(pinotHelixProperties);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(_serverInstance.getInstanceDataManager(),
            new ColumnarSegmentMetadataLoader());
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixManager.connect();
    _helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
        CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
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

  public static void main(String[] args) throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    int port = 8001;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, port);
    configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
    final HelixServerStarter pinotHelixStarter =
        new HelixServerStarter("sprintDemoCluster", "localhost:2181", configuration);
  }
}
