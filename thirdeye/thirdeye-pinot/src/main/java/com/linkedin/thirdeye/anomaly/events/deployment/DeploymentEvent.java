package com.linkedin.thirdeye.anomaly.events.deployment;
import com.linkedin.thirdeye.anomaly.events.Event;

public class DeploymentEvent extends Event {
  String status;

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
