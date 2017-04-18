package com.linkedin.thirdeye.rootcause;

/**
 * Interface for a named stateless pipeline as injected into {@code Framework}. Holds the business logic for
 * associating search context entities with other relevant entities. Also performs relative ranking
 * of associated entities in terms of importance to the user.
 *
 * @see com.linkedin.thirdeye.rootcause.Framework
 */
public interface Pipeline {

  /**
   * Returns the name of the pipeline which is both a unique identifier within a {@code Framework}
   * instance and a human-readable identifier.
   *
   * @return pipeline identifier
   */
  String getName();

  /**
   * Executes the pipeline given the execution context set up by the Framework. Returns entities
   * as determined relevant given the user-specified search context (contained in the execution
   * context).
   *
   * @param context execution context
   * @return pipeline results
   */
  PipelineResult run(ExecutionContext context);
}
