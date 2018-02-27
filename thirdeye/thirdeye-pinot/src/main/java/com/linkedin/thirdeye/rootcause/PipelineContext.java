package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.impl.EntityUtils;
import java.util.Map;
import java.util.Set;


/**
 * Container object for the execution context (the state) of {@code Pipeline.run()}. Holds the search context
 * with user-specified entities as well as the (incremental) results from executing individual
 * pipelines.
 */
public class PipelineContext {
  private final Map<String, Set<Entity>> inputs;

  public PipelineContext(Map<String, Set<Entity>> inputs) {
    this.inputs = inputs;
  }

  /**
   * Returns a map of sets of entities that were generated as the output of upstream (input)
   * pipelines. The map is keyed by pipeline id.
   *
   * @return Map of input entities, keyed by generating pipeline id
   */
  public Map<String, Set<Entity>> getInputs() {
    return inputs;
  }

  /**
   * Flattens the inputs from different pipelines and filters them by (super) class {@code clazz}.
   * Returns a set of typed Entities or an empty set if no matching instances are found.  URN
   * conflicts are resolved by preserving the entity with the highest score.
   *
   * @param clazz (super) class to filter by
   * @param <T> (super) class of output collection
   * @return set of Entities in input context with given super class
   */
  public <T extends Entity> Set<T> filter(Class<? extends T> clazz) {
    Set<T> filtered = new MaxScoreSet<>();
    for(Set<Entity> entities : this.inputs.values()) {
      filtered.addAll(EntityUtils.filter(entities, clazz));
    }
    return filtered;
  }

}
