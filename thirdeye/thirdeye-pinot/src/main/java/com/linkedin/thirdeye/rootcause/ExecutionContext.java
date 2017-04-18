package com.linkedin.thirdeye.rootcause;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Container object for the execution context (the state) of {@code RCAFramework.run()}. Holds the search context
 * with user-specified entities as well as the (incremental) results from executing individual
 * pipelines.
 */
public class ExecutionContext {
  final SearchContext searchContext;
  final Map<String, PipelineResult> results;

  public ExecutionContext(SearchContext searchContext) {
    this.searchContext = searchContext;
    this.results = new HashMap<>();
  }

  public ExecutionContext(SearchContext searchContext, Map<String, PipelineResult> results) {
    this.searchContext = searchContext;
    this.results = results;
  }

  public SearchContext getSearchContext() {
    return searchContext;
  }

  public Map<String, PipelineResult> getResults() {
    return Collections.unmodifiableMap(results);
  }
}
