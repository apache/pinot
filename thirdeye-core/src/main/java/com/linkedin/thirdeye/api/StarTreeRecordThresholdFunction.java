package com.linkedin.thirdeye.api;

import java.util.Properties;

public interface StarTreeRecordThresholdFunction
{
  /**
   * Initializes this threshold function with any necessary state.
   */
  void init(Properties props);

  /**
   * Determines whether or not a set of records pass a threshold.
   *
   * <p>
   *   For example, when a split occurs on dimension D, we compute the lists of all records in that leaf
   *   with dimension value d_i, then for each list, if the list passes the threshold, we represent d_i explicitly
   *   as a child node. Otherwise, d_i is rolled up into the other (?) node.
   * </p>
   *
   * @param records
   *  A list of records to test
   * @return
   *  True if the records pass some threshold.
   */
  boolean passesThreshold(Iterable<StarTreeRecord> records);
}
