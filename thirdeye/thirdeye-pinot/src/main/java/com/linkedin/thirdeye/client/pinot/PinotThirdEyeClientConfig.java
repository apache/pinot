package com.linkedin.thirdeye.client.pinot;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.linkedin.thirdeye.client.Client;
import com.linkedin.thirdeye.client.ClientConfig;

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


  @Override
  public String toString() {
    ToStringHelper stringHelper = MoreObjects.toStringHelper(PinotThirdEyeClientConfig.class);
    stringHelper.add("brokerUrl", brokerUrl).add("clusterName", clusterName)
        .add("controllerHost", controllerHost).add("controllerPort", controllerPort)
        .add("zookeeperUrl", zookeeperUrl);
    return stringHelper.toString();
  }

  /**
   * Returns pinot thirdeye client config from all client config. There can be only ONE client of each type
   * @param clientConfig
   * @return
   */
  public static PinotThirdEyeClientConfig createThirdeyeClientConfig(ClientConfig clientConfig) {
    PinotThirdEyeClientConfig config = null;
    for (Client client : clientConfig.getClients()) {
      if (client.getClassName().equals(PinotThirdEyeClient.class.getCanonicalName())) {
        if (config == null) {
          Map<String, String> properties = client.getProperties();
          String controllerHost = properties.get(PinotThirdeyeClientProperties.CONTROLLER_HOST.getValue());
          int controllerPort = Integer.valueOf(properties.get(PinotThirdeyeClientProperties.CONTROLLER_PORT.getValue()));
          String clusterName = properties.get(PinotThirdeyeClientProperties.CLUSTER_NAME.getValue());
          String zookeeperUrl = properties.get(PinotThirdeyeClientProperties.ZOOKEEPER_URL.getValue());
          String brokerUrl = properties.get(PinotThirdeyeClientProperties.BROKER_URL.getValue());
          String tag = properties.get(PinotThirdeyeClientProperties.TAG.getValue());

          config = new PinotThirdEyeClientConfig();
          config.setControllerHost(controllerHost);
          config.setControllerPort(controllerPort);
          config.setClusterName(clusterName);
          config.setZookeeperUrl(zookeeperUrl);
          if (StringUtils.isNotBlank(brokerUrl)) {
            config.setBrokerUrl(brokerUrl);
          }
          if (StringUtils.isNotBlank(tag)) {
            config.setTag(tag);
          }
        } else {
          throw new IllegalStateException("Found another client of type PinotThirdEyeClient. "
              + "There can only be ONE client of each type" + clientConfig);
        }
      }
    }
    return config;
  }
}
