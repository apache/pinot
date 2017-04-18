package com.linkedin.thirdeye.rootcause;

import java.util.List;
import java.util.Map;


/**
 * Container object for framework execution results. Holds the results aggregated across all
 * pipeline executions, the results for each individual pipeline, as well as the execution
 * context.
 *
 */
public final class FrameworkResult {
  final List<Entity> aggregatedResults;
  final Map<String, PipelineResult> pipelineResults;
  final ExecutionContext context;

  public FrameworkResult(List<Entity> aggregatedResults, Map<String, PipelineResult> pipelineResults,
      ExecutionContext context) {
    this.aggregatedResults = aggregatedResults;
    this.pipelineResults = pipelineResults;
    this.context = context;
  }

  public List<Entity> getAggregatedResults() {
    return aggregatedResults;
  }

  public Map<String, PipelineResult> getPipelineResults() {
    return pipelineResults;
  }

  public ExecutionContext getContext() {
    return context;
  }
}
