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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

/**
 * This is the implementation of the WO4W Average function or checking data completeness of datasets
 */
public class Wo4WAvgDataCompletenessAlgorithm implements DataCompletenessAlgorithm {

  public static double DEFAULT_EXPECTED_COMPLETENESS = 80;
  private static double CONSIDER_COMPLETE_AFTER = 95;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(Wo4WAvgDataCompletenessAlgorithm.class);

  private DataCompletenessConfigManager dataCompletenessConfigDAO = null;

  public Wo4WAvgDataCompletenessAlgorithm() {
    dataCompletenessConfigDAO = DAO_REGISTRY.getDataCompletenessConfigDAO();
  }


  @Override
  public void computeBaselineCountsIfNotPresent(String dataset, Map<String, Long> bucketNameToBucketValueMS,
      DateTimeFormatter dateTimeFormatter, TimeSpec timeSpec, DateTimeZone zone) {

    // look for the past 4 weeks
    for (int i = 0; i < 4; i ++) {
      Period baselineOffsetPeriod = new Period(0, 0, 0, 7*(i+1), 0, 0, 0, 0);
      LOG.info("Checking for {} week ago for dataset {}", (i+1), dataset);

      // check if baseline is present in database
      Map<String, Long> baselineBucketNameToBucketValueMS = new HashMap<>();
      for (Entry<String, Long> entry : bucketNameToBucketValueMS.entrySet()) {
        DateTime bucketValueDateTime = new DateTime(entry.getValue(), zone);
        Long baselineBucketValueMS = bucketValueDateTime.minus(baselineOffsetPeriod).getMillis();
        String baselineBucketName = dateTimeFormatter.print(baselineBucketValueMS);
        DataCompletenessConfigDTO configDTO = dataCompletenessConfigDAO.findByDatasetAndDateSDF(dataset, baselineBucketName);
        if (configDTO == null) {
          baselineBucketNameToBucketValueMS.put(baselineBucketName, baselineBucketValueMS);
        }
      }
      // for all baseline values not present in database, fetch their counts, and update in database
      LOG.info("Missing baseline buckets {} for dataset {}", baselineBucketNameToBucketValueMS.keySet(), dataset);
      if (!baselineBucketNameToBucketValueMS.isEmpty()) {

        Map<String, Long> baselineCountsForBuckets =
            DataCompletenessUtils.getCountsForBucketsOfDataset(dataset, timeSpec, baselineBucketNameToBucketValueMS);
        LOG.info("Baseline bucket counts {}", baselineCountsForBuckets);

        for (Entry<String, Long> entry : baselineCountsForBuckets.entrySet()) {
          String baselineBucketName = entry.getKey();
          Long baselineBucketCount = entry.getValue();
          Long baselineBucketValueMS = baselineBucketNameToBucketValueMS.get(baselineBucketName);

          DataCompletenessConfigDTO createBaselineConfig = new DataCompletenessConfigDTO();
          createBaselineConfig.setDataset(dataset);
          createBaselineConfig.setDateToCheckInSDF(baselineBucketName);
          createBaselineConfig.setDateToCheckInMS(baselineBucketValueMS);
          createBaselineConfig.setCountStar(baselineBucketCount);
          dataCompletenessConfigDAO.save(createBaselineConfig);
        }
        LOG.info("Saved {} number of baseline counts in database for dataset {}",
            baselineCountsForBuckets.size(), dataset);
      }
    }
  }

  @Override
  public List<Long> getBaselineCounts(String dataset, Long bucketValue) {
    long weekInMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    long baselineInMS = bucketValue;
    List<Long> baselineCounts = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      long count = 0;
      baselineInMS = baselineInMS - weekInMillis;
      DataCompletenessConfigDTO config = dataCompletenessConfigDAO.findByDatasetAndDateMS(dataset, baselineInMS);
      if (config != null) {
        count = config.getCountStar();
      }
      baselineCounts.add(count);
    }
    return baselineCounts;
  }



  @Override
  public Map<String, Long> getCurrentCountsForBuckets(String dataset, TimeSpec timeSpec,
      Map<String, Long> bucketNameToBucketValueMS) {
    return DataCompletenessUtils.getCountsForBucketsOfDataset(dataset, timeSpec, bucketNameToBucketValueMS);
  }

  @Override
  public double getPercentCompleteness(List<Long> baselineCounts, Long currentCount) {
    double percentCompleteness = 0;
    double baselineTotalCount = 0;
    for (Long baseline : baselineCounts) {
      baselineTotalCount = baselineTotalCount + baseline;
    }
    baselineTotalCount = baselineTotalCount/baselineCounts.size();
    if (baselineTotalCount != 0) {
      percentCompleteness = new Double(currentCount * 100) / baselineTotalCount;
    }
    if (baselineTotalCount == 0 && currentCount != 0) {
      percentCompleteness = 100;
    }
    return percentCompleteness;
  }

  @Override
  public boolean isDataComplete(Double percentComplete, Double expectedCompleteness) {
    boolean isDataComplete = false;
    if (expectedCompleteness == null) {
      expectedCompleteness = DEFAULT_EXPECTED_COMPLETENESS;
    }
    if (percentComplete >= expectedCompleteness) {
      isDataComplete = true;
    }
    return isDataComplete;
  }

  @Override
  public double getConsiderCompleteAfter() {
    return CONSIDER_COMPLETE_AFTER;
  }


}
