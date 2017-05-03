package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.EventEntity;


public class EventEntityFormatter extends RootCauseEntityFormatter {
  @Override
  public boolean applies(Entity entity) {
    return EventEntity.TYPE.isType(entity.getUrn());
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    EventEntity e = EventEntity.fromURN(entity.getUrn(), entity.getScore());

    String label = String.format("%s (%s)", e.getType(), e.getId());
    String link = String.format("javascript:alert('%s');", entity.getUrn());

    return makeRootCauseEntity(entity, "Event", label, link);
  }
}
