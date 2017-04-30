package com.linkedin.thirdeye.client.pinot;

import java.io.File;
import java.io.FileInputStream;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.constructor.Constructor;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class PinotThirdEyeClientConfig {

  String zookeeperUrl;

  String controllerHost;

  int controllerPort;

  String clusterName;

  String brokerUrl;

  String tag;

  public String getZookeeperUrl() {
    return zookeeperUrl;
  }

  public void setZookeeperUrl(String zookeeperUrl) {
    this.zookeeperUrl = zookeeperUrl;
  }

  public String getControllerHost() {
    return controllerHost;
  }

  public void setControllerHost(String controllerHost) {
    this.controllerHost = controllerHost;
  }

  public int getControllerPort() {
    return controllerPort;
  }

  public void setControllerPort(int controllerPort) {
    this.controllerPort = controllerPort;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getBrokerUrl() {
    return brokerUrl;
  }

  public void setBrokerUrl(String brokerUrl) {
    this.brokerUrl = brokerUrl;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public static PinotThirdEyeClientConfig fromFile(File dataSourceFile) throws Exception {
    return (PinotThirdEyeClientConfig) new Yaml(new Constructor(PinotThirdEyeClientConfig.class))
        .load(new FileInputStream(dataSourceFile));
  }

  @Override
  public String toString() {
    ToStringHelper stringHelper = MoreObjects.toStringHelper(PinotThirdEyeClientConfig.class);
    stringHelper.add("brokerUrl", brokerUrl).add("clusterName", clusterName)
        .add("controllerHost", controllerHost).add("controllerPort", controllerPort)
        .add("zookeeperUrl", zookeeperUrl);
    return stringHelper.toString();
  }


  public static PinotThirdEyeClientConfig createThirdEyeClientConfig(ThirdEyeConfiguration config) throws Exception {
    File clientConfigDir = new File(config.getRootDir(), "client-config");
    File clientConfigFile = new File(clientConfigDir, config.getClient() + ".yml");
    PinotThirdEyeClientConfig thirdEyeClientConfig =
        PinotThirdEyeClientConfig.fromFile(clientConfigFile);
    return thirdEyeClientConfig;
  }
}
