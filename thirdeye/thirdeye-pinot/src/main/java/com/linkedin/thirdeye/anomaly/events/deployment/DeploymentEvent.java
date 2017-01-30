package com.linkedin.thirdeye.anomaly.events.deployment;

import com.linkedin.thirdeye.anomaly.events.ExternalEvent;

public class DeploymentEvent extends ExternalEvent {
  String serviceName;

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }
}
