package com.linkedin.thirdeye.completeness.checker;

import java.util.List;

/**
 * This will serve as the interface for any algorithm we plug in to the completeness checker
 */
public interface DataCompletenessAlgorithm {

  /**
   * fetch all required baseline values for the dataset, corresponding to the bucketvalue being checked
   * @param dataset
   * @param bucketValue
   * @return
   */
  public List<Long> getBaselineCounts(String dataset, Long bucketValue);

  /**
   * Given the baseline counts and the current count, find out the percent completeness
   * @param baselineCounts
   * @param currentCount
   * @return
   */
  public double getPercentCompleteness(List<Long> baselineCounts, Long currentCount);

  /**
   * Verify whether the data completeness percentage passes the expectations
   * @param percentComplete
   * @param expectedCompleteness
   * @return
   */
  public boolean isDataComplete(Double percentComplete, Double expectedCompleteness);

  /**
   * This method will return the percentage after which we can consider that the entry is complete, and doesn't need to be checked again
   * This percentage should be typically higher than the expectedCompleteness.
   * Even after an entry has passed expected completeness and been marked as complete,
   * we will continue to check it, in case the percentage has improved.
   * We want to avoid looking at datasets after they've reached ~100%
   *
   * @return
   */
  public double getConsiderCompleteAfter();


}
