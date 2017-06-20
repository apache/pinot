package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;


public class DimensionEntityFormatter extends RootCauseEntityFormatter {
  public static final String TYPE_DIMENSION = "dimension";

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof DimensionEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    DimensionEntity e = (DimensionEntity) entity;
    String label = String.format("%s=%s", e.getName(), e.getValue());
    return makeRootCauseEntity(entity, TYPE_DIMENSION, label, null);
  }
}
