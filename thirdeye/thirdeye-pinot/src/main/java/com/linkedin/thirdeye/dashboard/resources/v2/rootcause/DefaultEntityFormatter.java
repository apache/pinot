package com.linkedin.thirdeye.dashboard.resources.v2.rootcause;

import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;


/**
 * Default formatter that applies to any Entity. Provides minimal information and serves as
 * a fallback.
 */
public class DefaultEntityFormatter extends RootCauseEntityFormatter {
  @Override
  public boolean applies(Entity entity) {
    return true;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    return makeRootCauseEntity(entity, "Other", "(none)", "javascript:void(0);");
  }
}
