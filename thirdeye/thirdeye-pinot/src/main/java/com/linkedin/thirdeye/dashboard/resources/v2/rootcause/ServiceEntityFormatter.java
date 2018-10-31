package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.ServiceEntity;


public class ServiceEntityFormatter extends RootCauseEntityFormatter {
  public static final String TYPE_SERVICE = "service";

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof ServiceEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    ServiceEntity e = (ServiceEntity) entity;
    return makeRootCauseEntity(entity, TYPE_SERVICE, e.getName(), null);
  }
}
