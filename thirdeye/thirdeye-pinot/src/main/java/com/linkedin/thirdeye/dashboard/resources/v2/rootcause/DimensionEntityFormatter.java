package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.DimensionEntity;


public class DimensionEntityFormatter extends RootCauseEntityFormatter {
  @Override
  public boolean applies(Entity entity) {
    return DimensionEntity.TYPE.isType(entity.getUrn());
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    DimensionEntity e = DimensionEntity.fromURN(entity.getUrn(), entity.getScore());

    String label = String.format("%s=%s", e.getName(), e.getValue());
    String link = String.format("javascript:alert('%s');", e.getUrn());

    return makeRootCauseEntity(entity, "Dimension", label, link);
  }
}
