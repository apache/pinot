package com.linkedin.thirdeye.anomaly.events.deployment;

import com.linkedin.thirdeye.anomaly.events.EventDataProvider;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import java.util.List;

public class DefaultDeploymentEventProvider implements EventDataProvider<DeploymentEvent> {
  @Override
  public List<DeploymentEvent> getEvents(EventFilter eventFilter) {
    return null;
  }
}
