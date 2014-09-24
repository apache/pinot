package com.linkedin.pinot.controller.helix.api;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.model.InstanceConfig;


public class PinotInstance {
  private Map<String, String> instanceConfigs;
  private String host;
  private String port;

  public PinotInstance() {
    instanceConfigs = new HashMap<String, String>();
  }

  public Map<String, String> getInstanceConfigs() {
    return instanceConfigs;
  }

  public void setInstanceConfigs(Map<String, String> instanceConfigs) {
    this.instanceConfigs = instanceConfigs;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getInstanceName() {
    String[] ret = { host, port };
    return StringUtils.join(ret, "_");
  }

  public InstanceConfig toInstanceConfig() {
    InstanceConfig config = new InstanceConfig(getInstanceName());
    config.getRecord().setSimpleFields(instanceConfigs);
    config.setHostName(host);
    config.setPort(port);
    config.setInstanceEnabled(true);

    return config;
  }

  @Override
  public String toString() {
    return host + " , " + port;
  }
}
