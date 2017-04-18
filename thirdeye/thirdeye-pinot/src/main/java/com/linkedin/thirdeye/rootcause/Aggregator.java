package com.linkedin.thirdeye.rootcause;

import java.util.List;
import java.util.Map;


/**
 * Interface for the stateless result aggregator as used by {@code Framework.run()} to linearize results
 * across multiple pipelines. An aggregator should normalize scores across different pipelines
 * and handle cases where the same entity is returned multiple times by different pipelines.
 */
public interface Aggregator {
  /**
   * Aggregates and linearizes results from multiple named pipelines. The method should normalize scores across
   * different pipelines to make them comparable and handle cases where the same entity (URN) is
   * returned as a result by multiple pipelines. May also drop irrelevant results.
   *
   * @param results pipeline results mapped by pipeline identifier
   * @return list of entities sorted by decreasing importance to the user
   */
  List<Entity> aggregate(Map<String, PipelineResult> results);
}
