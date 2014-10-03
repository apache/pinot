package com.linkedin.pinot.broker.broker.helix;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;


public class DefaultHelixBrokerConfig {

  public static Configuration getDefaultBrokerConf() {
    Configuration brokerConf = new PropertiesConfiguration();

    // config based routing
    brokerConf.addProperty("pinot.broker.transport.routingMode", "CONFIG");

    // no resources
    // serverConf.addProperty("pinot.broker.transport.routing.resourceName", "mirror");

    // serverConf.addProperty("pinot.broker.transport.routing.mirror.numNodesPerReplica", "1");
    // serverConf.addProperty("pinot.broker.transport.routing.mirror.serversForNode.0", "localhost:8098");
    // serverConf.addProperty("pinot.broker.transport.routing.mirror.serversForNode.default", "localhost:8098");

    //client properties
    brokerConf.addProperty("pinot.broker.client.enableConsole", "true");
    brokerConf.addProperty("pinot.broker.client.queryPort", "8099");
    brokerConf.addProperty("pinot.broker.client.consolePath", "/home/dpatel/experiments/github/pinot/webapp");

    return brokerConf;
  }
}
