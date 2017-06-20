package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEventEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEventEntity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;


public class DefaultEventEntityFormatter extends RootCauseEventEntityFormatter {

  @Override
  public boolean applies(EventEntity entity) {
    return true;
  }

  @Override
  public RootCauseEventEntity format(EventEntity entity) {
    String label = String.format("%s %d", entity.getEventType(), entity.getId());
    return makeRootCauseEventEntity(entity, label, null, -1, -1, null);
  }
}
