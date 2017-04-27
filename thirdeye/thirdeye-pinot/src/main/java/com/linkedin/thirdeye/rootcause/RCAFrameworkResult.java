package com.linkedin.thirdeye.rootcause;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Container object for framework execution results. Holds the results aggregated across all
 * pipeline executions as well as the results for each individual pipeline (keyed by pipeline name).
 *
 */
public final class RCAFrameworkResult {
  final Set<Entity> results;
  final Map<String, PipelineResult> pipelineResults;

  public RCAFrameworkResult(Set<? extends Entity> results, Map<String, PipelineResult> pipelineResults) {
    this.results = new HashSet<>(results);
    this.pipelineResults = pipelineResults;
  }

  public Set<Entity> getResults() {
    return results;
  }

  public Map<String, PipelineResult> getPipelineResults() {
    return pipelineResults;
  }
}
