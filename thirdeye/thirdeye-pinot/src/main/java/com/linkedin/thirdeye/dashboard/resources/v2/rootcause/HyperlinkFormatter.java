package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.HyperlinkEntity;


public class HyperlinkFormatter extends RootCauseEntityFormatter {
  public static final String TYPE_HYPERLINK = "hyperlink";

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof HyperlinkEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    HyperlinkEntity e = (HyperlinkEntity) entity;
    return makeRootCauseEntity(entity, TYPE_HYPERLINK, e.getUrl(), e.getUrl());
  }
}
