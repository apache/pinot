package com.linkedin.pinot.controller;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 26, 2014
 */

public class ControllerConf extends PropertiesConfiguration {
  private static final String CONTROLLER_HOST = "controller.host";
  private static final String CONTROLLER_PORT = "controller.port";
  private static final String DATA_DIR = "controller.data.dir";
  private static final String ZK_STR = "controller.zk.str";

  public ControllerConf(File file) throws ConfigurationException {
    super(file);
  }

  public ControllerConf() {
    super();
  }

  public void setControllerHost(String host) {
    setProperty(CONTROLLER_HOST, host);
  }

  public void setControllerPort(String port) {
    setProperty(CONTROLLER_PORT, port);
  }

  public void setDataDir(String dataDir) {
    setProperty(DATA_DIR, dataDir);
  }

  public void setZkStr(String zkStr) {
    setProperty(ZK_STR, zkStr);
  }

  public String getControllerHost() {
    return (String) getProperty(CONTROLLER_HOST);
  }

  public String getControllerPort() {
    return (String) getProperty(CONTROLLER_PORT);
  }

  public String getDataDir() {
    return (String) getProperty(DATA_DIR);
  }

  public String getZkStr() {
    return (String) getProperty(ZK_STR);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
