package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;


/**
 * Foundation class for building UI formatters for RCA Entities. Takes in RCA Entities and returns
 * RootCauseEntity container instances that contain human-readable data for display on the GUI.
 */
public abstract class RootCauseEntityFormatter {
  public abstract boolean applies(Entity entity);

  public abstract RootCauseEntity format(Entity entity);

  public static RootCauseEntity makeRootCauseEntity(Entity entity, String type, String label, String link) {
    RootCauseEntity out = new RootCauseEntity();
    out.setUrn(entity.getUrn());
    out.setScore(entity.getScore());
    out.setType(type);
    out.setLabel(label);
    out.setLink(link);
    return out;
  }
}
