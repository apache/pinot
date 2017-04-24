package com.linkedin.thirdeye.rootcause;

import java.util.Map;
import java.util.Set;


/**
 * Container object for the execution context (the state) of {@code Pipeline.run()}. Holds the search context
 * with user-specified entities as well as the (incremental) results from executing individual
 * pipelines.
 */
public class PipelineContext {
  final Map<String, Set<Entity>> inputs;

  public PipelineContext(Map<String, Set<Entity>> inputs) {
    this.inputs = inputs;
  }

  public Map<String, Set<Entity>> getInputs() {
    return inputs;
  }
}
