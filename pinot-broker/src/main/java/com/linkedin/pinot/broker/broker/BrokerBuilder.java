package com.linkedin.pinot.broker.broker;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.Server;


/**
 * @author dpatel
 *
 */
public class BrokerBuilder {

  private int port;
  private Server server;
  
  public BrokerBuilder(PropertiesConfiguration configuration) {
    port = 8089;
  }

  public void build() {
    // build transport
    // build server which has servlet
    // inject broker into the servlet lifecuvle
    Server s = new Server(port);
    
  }
  
  public void start() {
    // transport start
    // broker start
  }
  
  public void stop() {
    
  }
}
