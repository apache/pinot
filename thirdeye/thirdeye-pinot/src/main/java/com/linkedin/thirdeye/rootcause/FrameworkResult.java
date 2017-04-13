package com.linkedin.thirdeye.rootcause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public final class FrameworkResult {
  final List<Entity> entities;
  final ExecutionContext context;

  public FrameworkResult(List<? extends Entity> entities, ExecutionContext context) {
    this.entities = new ArrayList<>(entities);
    this.context = context;
  }

  public List<Entity> getEntities() {
    return Collections.unmodifiableList(entities);
  }

  public ExecutionContext getContext() {
    return context;
  }
}
