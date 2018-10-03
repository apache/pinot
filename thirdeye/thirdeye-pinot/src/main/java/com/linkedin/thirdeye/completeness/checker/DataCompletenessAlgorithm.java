/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.completeness.checker;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.thirdeye.api.TimeSpec;

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
  List<Long> getBaselineCounts(String dataset, Long bucketValue);

  /**
   * Fetch current counts for all buckets of this dataset
   * @param dataset
   * @param timeSpec
   * @param bucketNameToBucketValueMS
   * @return
   */
  Map<String, Long> getCurrentCountsForBuckets(String dataset, TimeSpec timeSpec, Map<String, Long> bucketNameToBucketValueMS);

  /**
   * Given the baseline counts and the current count, find out the percent completeness
   * @param baselineCounts
   * @param currentCount
   * @return
   */
  double getPercentCompleteness(List<Long> baselineCounts, Long currentCount);

  /**
   * Verify whether the data completeness percentage passes the expectations
   * @param percentComplete
   * @param expectedCompleteness
   * @return
   */
  boolean isDataComplete(Double percentComplete, Double expectedCompleteness);

  /**
   * This method will return the percentage after which we can consider that the entry is complete, and doesn't need to be checked again
   * This percentage should be typically higher than the expectedCompleteness.
   * Even after an entry has passed expected completeness and been marked as complete,
   * we will continue to check it, in case the percentage has improved.
   * We want to avoid looking at datasets after they've reached ~100%
   *
   * @return
   */
  double getConsiderCompleteAfter();

  /**
   * This method will help in the case of cold start.
   * When system starts, or a new dataset is added, baseline entries will not be present.
   * This method checks if required baseline entries are available,
   * and if not available, computes and stores them, for the rest of the computation to use
   * @param dataset
   * @param bucketNameToBucketValueMS
   * @param dateTimeFormatter
   * @param timeSpec
   * @param zone
   */
  void computeBaselineCountsIfNotPresent(String dataset, Map<String, Long> bucketNameToBucketValueMS,
      DateTimeFormatter dateTimeFormatter, TimeSpec timeSpec, DateTimeZone zone);


}
