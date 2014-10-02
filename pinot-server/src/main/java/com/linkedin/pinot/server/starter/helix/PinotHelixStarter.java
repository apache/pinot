package com.linkedin.pinot.server.starter.helix;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;
import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineHandlerFactory.SegmentOnlineOfflineTransitionHandler;


public class PinotHelixStarter {

  private static AtomicInteger counter = new AtomicInteger(0);
  private static String UNTAGGED = "untagged";
  private final HelixManager _helixManager;
  private final Configuration _pinotHelixProperties;

  private static ServerConf _serverConf;
  private static ServerInstance _serverInstance;
  private static final Logger LOGGER = LoggerFactory.getLogger("PinotHelixStarter");

  public PinotHelixStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {

    _pinotHelixProperties = pinotHelixProperties;
    final String instanceId = pinotHelixProperties.getString("instanceId");

    pinotHelixProperties.addProperty("pinot.server.instance.id", instanceId);
    //startServerInstance(pinotHelixProperties);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer
            + "/pinot-helix");
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    final StateTransitionHandlerFactory<SegmentOnlineOfflineTransitionHandler> transitionHandlerFactory =
        new SegmentOnlineOfflineHandlerFactory();
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineHandlerFactory.getStateModelDefId(),
        transitionHandlerFactory);
    _helixManager.connect();
    _helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, UNTAGGED);
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

  private String getPort() {
    return (counter.incrementAndGet()) + "";
  }

  private String getHost() {
    return "localhost";
  }
}
