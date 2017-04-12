package com.linkedin.thirdeye.rootcause;

import java.util.List;


public final class FrameworkResult {
  final List<Entity> entities;
  final ExecutionContext context;

  public FrameworkResult(List<Entity> entities, ExecutionContext context) {
    this.entities = entities;
    this.context = context;
  }

  public List<Entity> getEntities() {
    return entities;
  }

  public ExecutionContext getContext() {
    return context;
  }
}
