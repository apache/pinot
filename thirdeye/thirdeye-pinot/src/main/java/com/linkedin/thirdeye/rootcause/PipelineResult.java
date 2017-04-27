package com.linkedin.thirdeye.rootcause;

import java.util.HashSet;
import java.util.Set;


/**
 * Container object for pipeline execution results. Holds entities with scores as set by the pipeline.
 */
public class PipelineResult {
  private final PipelineContext context;
  private final Set<Entity> entities;

  public PipelineResult(PipelineContext context, Set<? extends Entity> entities) {
    this.context = context;
    this.entities = new HashSet<>(entities);
  }

  public PipelineContext getContext() {
    return context;
  }

  public Set<Entity> getEntities() {
    return entities;
  }
}
