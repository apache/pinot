package org.apache.pinot.query.context;

import java.util.Map;


/**
 * PlannerContext is a PlannerContext that holds all contextual information during planning phase.
 */
public class PlannerContext {
  private Map<String, String> options;

  public void setOptions(Map<String, String> options) {

  }

  public Map<String, String> getOptions() {
    return options;
  }
}
