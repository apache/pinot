package com.linkedin.pinot.broker.broker.helix;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;


/**
 * Helix Broker Startable
 * 
 * @author xiafu
 *
 */
public class HelixBrokerStarter {

  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final Configuration _pinotHelixProperties;
  private final HelixBrokerRoutingTable _helixBrokerRoutingTable;
  // private final BrokerServerBuilder _brokerServerBuilder;
  public static final String KEY_OF_UNTAGGED_BROKER_RESOURCE = "broker_untagged";
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;

  private static final Logger LOGGER = LoggerFactory.getLogger("HelixBrokerStarter");

  public HelixBrokerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {

    _pinotHelixProperties = pinotHelixProperties;
    String brokerId = pinotHelixProperties.getString("instanceId", "Broker_" + NetUtil.getHostAddress());
    _pinotHelixProperties.addProperty("pinot.broker.id", brokerId);
    _helixExternalViewBasedRouting = new HelixExternalViewBasedRouting();
    _helixBrokerRoutingTable = new HelixBrokerRoutingTable(_helixExternalViewBasedRouting);
    // _brokerServerBuilder = startBroker();
    startBroker();
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer
            + "/pinot-helix");
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    final StateModelFactory<?> stateModelFactory =
        new BrokerResourceOnlineOfflineStateModelFactory(_helixManager, _helixExternalViewBasedRouting);
    stateMachineEngine.registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(),
        stateModelFactory);
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _helixAdmin.addInstanceTag(helixClusterName, brokerId, KEY_OF_UNTAGGED_BROKER_RESOURCE);
    _helixManager.addExternalViewChangeListener(_helixBrokerRoutingTable);

  }

  private BrokerServerBuilder startBroker() throws Exception {
    Configuration config = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    final BrokerServerBuilder brokerServerBuilder = new BrokerServerBuilder(config, _helixExternalViewBasedRouting);
    brokerServerBuilder.buildNetwork();
    brokerServerBuilder.buildHTTP();
    brokerServerBuilder.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          brokerServerBuilder.stop();
        } catch (Exception e) {
          LOGGER.error(e.getMessage());
        }
      }
    });
    return brokerServerBuilder;
  }

  public HelixExternalViewBasedRouting getHelixExternalViewBasedRouting() {
    return _helixExternalViewBasedRouting;
  }

}
