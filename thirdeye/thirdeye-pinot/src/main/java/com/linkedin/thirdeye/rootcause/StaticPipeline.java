package com.linkedin.thirdeye.rootcause;

import java.util.Set;


/**
 * StaticPipeline emits a fixed set of entities as a result, regardless of the input. It is
 * used to encapsulate constants (such as user input) during framework execution.
 */
public class StaticPipeline extends Pipeline {
  private final Set<Entity> entities;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param entities entities to emit as result
   */
  public StaticPipeline(String outputName, Set<String> inputNames, Set<Entity> entities) {
    super(outputName, inputNames);
    this.entities = entities;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, this.entities);
  }
}
