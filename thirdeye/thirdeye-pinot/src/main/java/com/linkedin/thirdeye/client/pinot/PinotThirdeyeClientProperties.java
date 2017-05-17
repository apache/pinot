package com.linkedin.thirdeye.client.pinot;

public enum PinotThirdeyeClientProperties {
  CONTROLLER_HOST("controllerHost"),
  CONTROLLER_PORT("controllerPort"),
  CLUSTER_NAME("clusterName"),
  ZOOKEEPER_URL("zookeeperUrl"),
  TAG("tag"),
  BROKER_URL("brokerUrl");

  private final String value;

  private PinotThirdeyeClientProperties(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

}

