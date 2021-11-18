package org.apache.pinot.query;

import java.util.Map;


/**
 * For planner to hold contextual information such as global options.
 */
public interface PinotPlannerContext {

  void setOptions(Map<String, String> options);

  Map<String, String> getOptions();
}
