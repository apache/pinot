package com.linkedin.thirdeye.datasource.pinot;

import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An immutable configurations for setting up {@link PinotThirdEyeDataSource}'s connection to Pinot.
 */
public class PinotThirdEyeDataSourceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeDataSourceConfig.class);

  private String zookeeperUrl;
  private String controllerHost;
  private int controllerPort;
  private String clusterName;
  private String brokerUrl;
  private String tag;

  public String getZookeeperUrl() {
    return zookeeperUrl;
  }

  private void setZookeeperUrl(String zookeeperUrl) {
    this.zookeeperUrl = zookeeperUrl;
  }

  public String getControllerHost() {
    return controllerHost;
  }

  private void setControllerHost(String controllerHost) {
    this.controllerHost = controllerHost;
  }

  public int getControllerPort() {
    return controllerPort;
  }

  private void setControllerPort(int controllerPort) {
    this.controllerPort = controllerPort;
  }

  public String getTag() {
    return tag;
  }

  private void setTag(String tag) {
    this.tag = tag;
  }

  public String getBrokerUrl() {
    return brokerUrl;
  }

  private void setBrokerUrl(String brokerUrl) {
    this.brokerUrl = brokerUrl;
  }

  public String getClusterName() {
    return clusterName;
  }

  private void setClusterName(String clusterName) {
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
   * Returns pinot thirdeye datasource config given datasource config. There can be only ONE datasource of pinot type
   * @param dataSourceConfig
   * @return
   */
  public static PinotThirdEyeDataSourceConfig createFromDataSourceConfig(DataSourceConfig dataSourceConfig) {
    if (dataSourceConfig == null ||
        !dataSourceConfig.getClassName().equals(PinotThirdEyeDataSource.class.getCanonicalName())) {
      throw new IllegalStateException("Data source config is not of type pinot " + dataSourceConfig);
    }
    return createFromProperties(dataSourceConfig.getProperties());
  }

  /**
   * Returns PinotThirdEyeDataSourceConfig from the given property map.
   *
   * @param properties the properties to setup a PinotThirdEyeDataSourceConfig.
   *
   * @return a PinotThirdEyeDataSourceConfig.
   *
   * @throws IllegalStateException is thrown if the property map does not contain all necessary fields, i.e., controller
   *                               host and port, cluster name, and the URL to zoo keeper.
   */
  public static PinotThirdEyeDataSourceConfig createFromProperties(Map<String, String> properties) {
    if (!isValidProperties(properties)) {
      throw new IllegalStateException(
          "Invalid properties for data source " + PinotThirdEyeDataSource.DATA_SOURCE_NAME + " " + properties);
    }

    String controllerHost = properties.get(PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue());
    int controllerPort = Integer.valueOf(properties.get(PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue()));
    String clusterName = properties.get(PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue());
    String zookeeperUrl = properties.get(PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue());
    // brokerUrl and tag are optional
    String brokerUrl = properties.get(PinotThirdeyeDataSourceProperties.BROKER_URL.getValue());
    String tag = properties.get(PinotThirdeyeDataSourceProperties.TAG.getValue());

    PinotThirdEyeDataSourceConfig pinotDataSourceConfig = new PinotThirdEyeDataSourceConfig();
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
    return pinotDataSourceConfig;
  }

  /**
   * Checks if the given property map could be used to construct a PinotThirdEyeDataSourceConfig. The essential fields
   * are controller host and port, cluster name, and the URL to zoo keeper. This method prints out all missing essential
   * fields before returning false.
   *
   * @param properties the property map to be checked.
   *
   * @return true if the property map has all the essential fields.
   */
  private static boolean isValidProperties(Map<String, String> properties) {
    boolean valid = true;
    if (MapUtils.isEmpty(properties)) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing properties {}", properties);
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.CONTROLLER_HOST.getValue());
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.CONTROLLER_PORT.getValue());
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.ZOOKEEPER_URL.getValue());
    }
    if (!properties.containsKey(PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue())) {
      valid = false;
      LOG.error("PinotThirdEyeDataSource is missing required property {}", PinotThirdeyeDataSourceProperties.CLUSTER_NAME.getValue());
    }
    return valid;
  }
}
