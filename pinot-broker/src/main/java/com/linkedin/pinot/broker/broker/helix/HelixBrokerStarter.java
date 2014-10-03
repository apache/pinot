package com.linkedin.pinot.broker.broker.helix;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.common.utils.NetUtil;


public class HelixBrokerStarter {

  private static AtomicInteger counter = new AtomicInteger(0);
  private static String UNTAGGED = "untagged";
  private static String BROKER = "untagged";
  private final HelixManager _helixManager;
  private final Configuration _pinotHelixProperties;
  private final RoutingTableProvider _routingTableProvider;

  private static final Logger LOGGER = LoggerFactory.getLogger("PinotHelixStarter");

  public HelixBrokerStarter(String helixClusterName, String zkServer, Configuration pinotHelixProperties)
      throws Exception {

    _pinotHelixProperties = pinotHelixProperties;
    String brokerId = "Broker_" + NetUtil.getHostAddress();
    _pinotHelixProperties.addProperty("pinot.broker.id", brokerId);

    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.SPECTATOR, zkServer
            + "/pinot-helix");
    startBroker();
    _helixManager.connect();
    _routingTableProvider = new RoutingTableProvider();
    _helixManager.addExternalViewChangeListener(_routingTableProvider);
    _helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId, BROKER);
  }

  private void startBroker() throws Exception {
    Configuration config = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    final BrokerServerBuilder bld = new BrokerServerBuilder(config, _routingTableProvider);
    bld.buildNetwork();
    bld.buildHTTP();
    bld.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          bld.stop();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    });

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String command = br.readLine();
      if (command.equals("exit")) {
        bld.stop();
      }
    }

  }
}
