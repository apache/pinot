package com.linkedin.pinot.server.starter.helix;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;


public class PinotHelixStarter {

  private static AtomicInteger counter = new AtomicInteger(0);
  private static String UNTAGGED = "untagged";
  private final HelixManager _helixManager;
  private final Properties _pinotHelixProperties;

  public PinotHelixStarter(String helixClusterName, String zkServer, Properties pinotHelixProperties) throws Exception {

    _pinotHelixProperties = pinotHelixProperties;
    String instanceId =
        pinotHelixProperties.getProperty("pinot.instance.name" + "_" + "pinot.instance.port", getHost() + "_"
            + getPort());
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer
            + "/pinot-helix");
    StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory("PinotStateModel", new PinotHelixStateModelFactory(instanceId,
        helixClusterName));
    _helixManager.connect();
    _helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, UNTAGGED);

  }

  private String getPort() {
    return (counter.incrementAndGet()) + "";
  }

  private String getHost() {
    return "localhost";
  }
}
