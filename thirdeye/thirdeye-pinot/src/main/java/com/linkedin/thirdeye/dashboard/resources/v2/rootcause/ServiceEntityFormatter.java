package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.ServiceEntity;


public class ServiceEntityFormatter extends RootCauseEntityFormatter {
  @Override
  public boolean applies(Entity entity) {
    return ServiceEntity.TYPE.isType(entity.getUrn());
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    ServiceEntity e = ServiceEntity.fromURN(entity.getUrn(), entity.getScore());

    String link = String.format("javascript:alert('%s');", e.getUrn());

    return makeRootCauseEntity(entity, "Service", e.getName(), link);
  }
}
