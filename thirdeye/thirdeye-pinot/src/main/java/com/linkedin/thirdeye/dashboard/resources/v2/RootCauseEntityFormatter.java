package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;


public abstract class RootCauseEntityFormatter {
  public abstract boolean applies(Entity entity);

  public abstract RootCauseEntity format(Entity entity);

  public static RootCauseEntity makeRootCauseEntity(Entity entity, String type, String label, String link) {
    RootCauseEntity out = new RootCauseEntity();
    out.setUrn(entity.getUrn());
    out.setScore(Math.round(entity.getScore() * 1000) / 1000.0);
    out.setType(type);
    out.setLabel(label);
    out.setLink(link);
    return out;
  }
}
