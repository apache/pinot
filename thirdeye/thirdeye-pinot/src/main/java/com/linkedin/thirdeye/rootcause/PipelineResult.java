package com.linkedin.thirdeye.rootcause;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Container object for pipeline execution results. Holds entities with scores as set by the pipeline.
 */
public class PipelineResult {
  final Collection<Entity> entities;

  public PipelineResult(Collection<? extends Entity> entities) {
    this.entities = new ArrayList<>(entities);
  }

  public Collection<Entity> getEntities() {
    return entities;
  }
}
