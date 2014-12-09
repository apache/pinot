package com.linkedin.pinot.controller;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 26, 2014
 */

public class ControllerConf extends PropertiesConfiguration {
  private static final String CONTROLLER_VIP_HOST = "controller.vip.host";
  private static final String CONTROLLER_HOST = "controller.host";
  private static final String CONTROLLER_PORT = "controller.port";
  private static final String DATA_DIR = "controller.data.dir";
  private static final String ZK_STR = "controller.zk.str";
  private static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  private static final String CONSOLE_WEBAPP_ROOT_PATH = "controller.query.console";

  public ControllerConf(File file) throws ConfigurationException {
    super(file);
  }

  public ControllerConf() {
    super();
  }

  public void setQueryConsolePath(String path) {
    if (!path.startsWith("file://")) {
      path = "file://" + path;
    }
    setProperty(CONSOLE_WEBAPP_ROOT_PATH, path);
  }

  public String getQueryConsole() {
    if (containsKey(CONSOLE_WEBAPP_ROOT_PATH)) {
      return (String) getProperty(CONSOLE_WEBAPP_ROOT_PATH);
    }
    return "file://" + ControllerConf.class.getClassLoader().getResource("webapp").getFile();
  }

  public void setHelixClusterName(String clusterName) {
    setProperty(HELIX_CLUSTER_NAME, clusterName);
  }

  public void setControllerHost(String host) {
    setProperty(CONTROLLER_HOST, host);
  }

  public void setControllerVipHost(String vipHost) {
    setProperty(CONTROLLER_VIP_HOST, vipHost);
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

  public String getHelixClusterName() {
    return (String) getProperty(HELIX_CLUSTER_NAME);
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

  public String getControllerVipHost() {
    if (containsKey(CONTROLLER_VIP_HOST) && ((String) getProperty(CONTROLLER_VIP_HOST)).length() > 0) {
      return (String) getProperty(CONTROLLER_VIP_HOST);
    }
    return (String) getProperty(CONTROLLER_HOST);
  }
}
