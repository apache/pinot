package com.linkedin.thirdeye.rootcause;

import java.util.Set;


/**
 * StaticPipeline emits a fixed set of entities as a result. It is typically used to encapsulate
 * user input during framework execution.
 */
public class StaticPipeline extends Pipeline {
  private final Set<Entity> entities;

  /**
   * Constructor for dependency injection
   *
   * @param name pipeline name
   * @param inputs pipeline inputs
   * @param entities entities to emit as result
   */
  public StaticPipeline(String name, Set<String> inputs, Set<Entity> entities) {
    super(name, inputs);
    this.entities = entities;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, this.entities);
  }
}
