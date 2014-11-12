package com.linkedin.thirdeye.api;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public interface StarTreeRecordThresholdFunction
{
  /**
   * Initializes this threshold function with any necessary state.
   */
  void init(Properties config);

  /**
   * @return
   *  The configuration this function was initialized with
   */
  Properties getConfig();

  /**
   * Determines which dimension values pass a threshold.
   *
   * @param sample
   *  A map whose keys are values for a particular dimension, and whose values are
   *  the records with those dimension values
   * @return
   *  The set of dimension values which pass the threshold.
   */
  Set<String> apply(Map<String, List<StarTreeRecord>> sample);
}
