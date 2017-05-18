package com.linkedin.thirdeye.datasource.pinot;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.DataSources;

public class PinotThirdEyeDataSourceConfig {

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
    ToStringHelper stringHelper = MoreObjects.toStringHelper(PinotThirdEyeDataSourceConfig.class);
    stringHelper.add("brokerUrl", brokerUrl).add("clusterName", clusterName)
        .add("controllerHost", controllerHost).add("controllerPort", controllerPort)
        .add("zookeeperUrl", zookeeperUrl);
    return stringHelper.toString();
  }

  /**
   * Returns pinot thirdeye datasource config from all datasource configs. There can be only ONE datasource of each type
   * @param dataSources
   * @return
   */
  public static PinotThirdEyeDataSourceConfig createPinotThirdeyeDataSourceConfig(DataSources dataSources) {
    PinotThirdEyeDataSourceConfig pinotDataSourceConfig = null;
    for (DataSourceConfig dataSourceConfig : dataSources.getDataSourceConfigs()) {
      if (dataSourceConfig.getClassName().equals(PinotThirdEyeDataSource.class.getCanonicalName())) {
        if (pinotDataSourceConfig == null) {
          Map<String, String> properties = dataSourceConfig.getProperties();
          String controllerHost = properties.get(PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue());
          int controllerPort = Integer.valueOf(properties.get(PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue()));
          String clusterName = properties.get(PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue());
          String zookeeperUrl = properties.get(PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue());
          String brokerUrl = properties.get(PinotThirdeyeDataSourceProperties.BROKER_URL.getValue());
          String tag = properties.get(PinotThirdeyeDataSourceProperties.TAG.getValue());

          pinotDataSourceConfig = new PinotThirdEyeDataSourceConfig();
          pinotDataSourceConfig.setControllerHost(controllerHost);
          pinotDataSourceConfig.setControllerPort(controllerPort);
          pinotDataSourceConfig.setClusterName(clusterName);
          pinotDataSourceConfig.setZookeeperUrl(zookeeperUrl);
          if (StringUtils.isNotBlank(brokerUrl)) {
            pinotDataSourceConfig.setBrokerUrl(brokerUrl);
          }
          if (StringUtils.isNotBlank(tag)) {
            pinotDataSourceConfig.setTag(tag);
          }
        } else {
          throw new IllegalStateException("Found another data source of type PinotThirdEyeDataSource. "
              + "There can only be ONE data source of each type" + dataSources);
        }
      }
    }
    return pinotDataSourceConfig;
  }
}
