package com.linkedin.thirdeye.datasource.pinot;

public enum PinotThirdeyeDataSourceProperties {
  CONTROLLER_HOST("controllerHost"),
  CONTROLLER_PORT("controllerPort"),
  CONTROLLER_CONNECTION_SCHEME("controllerConnectionScheme"),
  CLUSTER_NAME("clusterName"),
  ZOOKEEPER_URL("zookeeperUrl"),
  TAG("tag"),
  BROKER_URL("brokerUrl");

  private final String value;

  PinotThirdeyeDataSourceProperties(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

}

