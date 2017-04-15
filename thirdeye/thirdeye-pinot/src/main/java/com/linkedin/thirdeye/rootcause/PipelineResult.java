package com.linkedin.thirdeye.rootcause;

import java.util.ArrayList;
import java.util.Collection;


public class PipelineResult {
  final Collection<Entity> entities;

  public PipelineResult(Collection<? extends Entity> entities) {
    this.entities = new ArrayList<>(entities);
  }

  public Collection<Entity> getEntities() {
    return entities;
  }
}
