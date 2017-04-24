package com.linkedin.thirdeye.rootcause;

import java.util.Set;


public class StaticPipeline extends Pipeline {
  final Set<Entity> entities;

  public StaticPipeline(String name, Set<String> inputs, Set<Entity> entities) {
    super(name, inputs);
    this.entities = entities;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, this.entities);
  }
}
