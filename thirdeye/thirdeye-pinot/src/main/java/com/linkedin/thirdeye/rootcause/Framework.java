package com.linkedin.thirdeye.rootcause;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Framework {
  final Map<String, Pipeline> pipelines;
  final Aggregator aggregator;

  public Framework(Iterable<Pipeline> pipelines, Aggregator aggregator) {
    this.pipelines = new HashMap<>();
    for(Pipeline p : pipelines) {
      if(this.pipelines.containsKey(p.getName()))
        throw new IllegalArgumentException(String.format("Already contains pipeline with name '%s'", p.getName()));
      this.pipelines.put(p.getName(), p);
    }
    this.aggregator = aggregator;
  }

  FrameworkResult run(SearchContext searchContext) {
    Map<String, PipelineResult> results = new HashMap<>();

    // independent execution
    for(Map.Entry<String, Pipeline> e : this.pipelines.entrySet()) {
      ExecutionContext context = new ExecutionContext(searchContext);
      results.put(e.getKey(), e.getValue().run(context));
    }

    List<Entity> aggregated = this.aggregator.aggregate(results);
    ExecutionContext context = new ExecutionContext(searchContext, results);

    FrameworkResult result = new FrameworkResult(aggregated, context);

    return result;
  }
}
