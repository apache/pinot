package org.apache.pinot.query.context;

import org.apache.pinot.query.QueryContext;
import java.util.Map;


/**
 * PlannerContext is a QueryContext that holds all contextual information during planning phase.
 */
public class PlannerContext implements QueryContext {
  private Map<String, String> options;

  @Override
  public void setOptions(Map<String, String> options) {

  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }
}
