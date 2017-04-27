package com.linkedin.thirdeye.rootcause;

import java.util.Set;


/**
 * Interface for a named stateless pipeline as injected into {@code RCAFramework}. Holds the business logic for
 * associating search context entities with other relevant entities. Also performs relative ranking
 * of associated entities in terms of importance to the user.
 *
 * @see RCAFramework
 */
public abstract class Pipeline {
  final String name;
  final Set<String> inputs;

  public Pipeline(String name, Set<String> inputs) {
    this.name = name;
    this.inputs = inputs;
  }

  public final String getName() {
    return name;
  }

  public final Set<String> getInputs() {
    return inputs;
  }

  /**
   * Executes the pipeline given the execution context set up by the RCAFramework. Returns entities
   * as determined relevant given the user-specified search context (contained in the execution
   * context).
   *
   * @param context pipeline execution context
   * @return pipeline results
   */
  public abstract PipelineResult run(PipelineContext context);

}