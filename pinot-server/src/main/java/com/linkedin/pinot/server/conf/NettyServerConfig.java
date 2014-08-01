package com.linkedin.pinot.server.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


public class NettyServerConfig {

  // Netty server port
  private static String NETTY_SERVER_PORT = "port";

  private Configuration _serverNettyConfig;

  public NettyServerConfig(Configuration serverNettyConfig) throws ConfigurationException {
    _serverNettyConfig = serverNettyConfig;
    if (!_serverNettyConfig.containsKey(NETTY_SERVER_PORT)) {
      throw new ConfigurationException("Cannot find Key : " + NETTY_SERVER_PORT);
    }
  }

  /**
   * @return Netty server port
   */
  public int getPort() {
    return _serverNettyConfig.getInt(NETTY_SERVER_PORT);
  }
}
