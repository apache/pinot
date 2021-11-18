package org.apache.pinot.query.planner;

import org.apache.pinot.query.PinotPlannerContext;
import java.util.Map;


public class QueryPlannerContext implements PinotPlannerContext {
  private Map<String, String> options;

  @Override
  public void setOptions(Map<String, String> options) {

  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }
}
