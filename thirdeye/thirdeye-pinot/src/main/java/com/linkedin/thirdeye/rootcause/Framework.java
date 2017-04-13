package com.linkedin.thirdeye.rootcause;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Framework {
  private static final Logger LOG = LoggerFactory.getLogger(Framework.class);

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

  public FrameworkResult run(SearchContext searchContext) {
    Map<String, PipelineResult> results = new HashMap<>();

    LOG.info("Using search context '{}'", searchContext.entities);

    // independent execution
    for(Map.Entry<String, Pipeline> e : this.pipelines.entrySet()) {
      LOG.info("Running pipeline '{}'", e.getKey());
      ExecutionContext context = new ExecutionContext(searchContext);
      PipelineResult result = e.getValue().run(context);
      results.put(e.getKey(), result);
      LOG.info("Got {} results", result.getScores().size());
    }

    LOG.info("Aggregating results from {} pipelines", results.size());
    List<Entity> aggregated = this.aggregator.aggregate(results);
    ExecutionContext context = new ExecutionContext(searchContext, results);
    LOG.info("Aggregated scores for {} entities", aggregated.size());

    FrameworkResult result = new FrameworkResult(aggregated, context);

    return result;
  }
}
