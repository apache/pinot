package com.linkedin.pinot.broker.broker;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.broker.broker.helix.DefaultHelixBrokerConfig;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;


public class TestHelixBrokerStarter {
  private String _zkServer = "localhost:2181";
  private String _helixClusterName = "pinotClusterOne";

  public void testBrokerStarter() throws Exception {
    Configuration pinotHelixProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    HelixBrokerStarter helixBrokerStarter = new HelixBrokerStarter(_helixClusterName, _zkServer, pinotHelixProperties);
  }
}
