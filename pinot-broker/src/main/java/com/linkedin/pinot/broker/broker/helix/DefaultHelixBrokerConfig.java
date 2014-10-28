package com.linkedin.pinot.broker.broker.helix;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;


public class DefaultHelixBrokerConfig {

  public static Configuration getDefaultBrokerConf() {
    Configuration brokerConf = new PropertiesConfiguration();

    // config based routing
    brokerConf.addProperty("pinot.broker.transport.routingMode", "HELIX");

    brokerConf.addProperty("pinot.broker.routing.table.builder.default.class", "Random");
    brokerConf.addProperty("pinot.broker.routing.table.builder.default.numOfRoutingTables", "10");
    brokerConf.addProperty("pinot.broker.routing.table.builder.resources", "mirror,midas");
    brokerConf.addProperty("pinot.broker.routing.table.builder.mirror.class", "Random");
    brokerConf.addProperty("pinot.broker.routing.table.builder.mirror.numOfRoutingTables", "20");
    brokerConf.addProperty("pinot.broker.routing.table.builder.midas.class", "Random");
    brokerConf.addProperty("pinot.broker.routing.table.builder.midas.numOfRoutingTables", "15");

    //client properties
    brokerConf.addProperty("pinot.broker.client.enableConsole", "true");
    brokerConf.addProperty("pinot.broker.client.queryPort", "8099");
    brokerConf.addProperty("pinot.broker.client.consolePath", "../webapp");

    return brokerConf;
  }
}
