package com.linkedin.pinot.broker.broker;

import org.apache.commons.configuration.Configuration;


public class BrokerClientConf {

  public static final String ENABLE_QUERY_CONSOLE = "enableConsole";
  public static final String CONSOLE_RESOURCES_PATH = "consolePath";
  public static final String QUERY_PORT = "queryPort";
  private static final int DEFAULT_QUERY_PORT = 8882;

  private Configuration config;

  public BrokerClientConf(Configuration config) {
    this.config = config;
  }

  public boolean enableConsole() {
    if (config.containsKey(ENABLE_QUERY_CONSOLE)) {
      return config.getBoolean(ENABLE_QUERY_CONSOLE);
    }
    return true;
  }

  public String getConsoleWebappPath() {
    if (config.containsKey(CONSOLE_RESOURCES_PATH)) {
      return config.getString(CONSOLE_RESOURCES_PATH);
    }
    return null;
  }

  public int getQueryPort() {
    if (config.containsKey(QUERY_PORT)) {
      return config.getInt(QUERY_PORT);
    }
    return DEFAULT_QUERY_PORT;
  }
}
