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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessType;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * Task runnner for data completeness tasks
 */
public class DataCompletenessTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessTaskRunner.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static ThirdEyeAnomalyConfiguration thirdeyeConfig = null;

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();
    DataCompletenessTaskInfo dataCompletenessTaskInfo = (DataCompletenessTaskInfo) taskInfo;
    DataCompletenessType dataCompletenessType = dataCompletenessTaskInfo.getDataCompletenessType();
    if (dataCompletenessType.equals(DataCompletenessType.CHECKER)) {
      executeCheckerTask(dataCompletenessTaskInfo);
    } else if (dataCompletenessType.equals(DataCompletenessType.CLEANUP)) {
      executeCleanupTask(dataCompletenessTaskInfo);
    } else {
      throw new UnsupportedOperationException("DataCompleteness task must be of type CHECKER/CLEANUP, found "
          + dataCompletenessType);
    }
    return null;
  }

  /**
   * Performs data completeness check on all datasets, for past LOOKBACK time, and records the information in database
   * @param dataCompletenessTaskInfo
   */
  private void executeCheckerTask(DataCompletenessTaskInfo dataCompletenessTaskInfo) {
    LOG.info("Execute data completeness checker task {}", dataCompletenessTaskInfo);
    try {
      List<String> datasets = dataCompletenessTaskInfo.getDatasetsToCheck();
      LOG.info("Datasets {}", datasets);

      // get start and end time
      long dataCompletenessStartTime = dataCompletenessTaskInfo.getDataCompletenessStartTime();
      long dataCompletenessEndTime = dataCompletenessTaskInfo.getDataCompletenessEndTime();
      LOG.info("StartTime {} i.e. {}", dataCompletenessStartTime, new DateTime(dataCompletenessStartTime));
      LOG.info("EndTime {} i.e. {}", dataCompletenessEndTime, new DateTime(dataCompletenessEndTime));

      Multimap<String, DataCompletenessConfigDTO> incompleteEntriesToNotify = ArrayListMultimap.create();
      for (String dataset : datasets) {
        try {

          DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
          LOG.info("Dataset {} {}", dataset, datasetConfig);

          String algorithmClass = datasetConfig.getDataCompletenessAlgorithm();
          Double expectedCompleteness = datasetConfig.getExpectedCompleteness();
          DataCompletenessAlgorithm dataCompletenessAlgorithm = DataCompletenessAlgorithmFactory.getDataCompletenessAlgorithmFromClass(algorithmClass);
          LOG.info("DataCompletenessAlgorithmClass: {}", algorithmClass);

          // get adjusted start time, bucket size and date time formatter, according to dataset granularity
          TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
          DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
          long adjustedStart =
              DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, dataCompletenessStartTime, dateTimeZone);
          long adjustedEnd =
              DataCompletenessUtils.getAdjustedTimeForDataset(timeSpec, dataCompletenessEndTime, dateTimeZone);
          long bucketSize = DataCompletenessUtils.getBucketSizeInMSForDataset(timeSpec);
          DateTimeFormatter dateTimeFormatter =
              DataCompletenessUtils.getDateTimeFormatterForDataset(timeSpec, dateTimeZone);
          LOG.info("Adjusted start:{} i.e. {} Adjusted end:{} i.e. {} and Bucket size:{}",
              adjustedStart, new DateTime(adjustedStart), adjustedEnd, new DateTime(adjustedEnd), bucketSize);

          // get buckets to process
          Map<String, Long> bucketNameToBucketValueMS = getBucketsToProcess(dataset, adjustedStart, adjustedEnd,
              dataCompletenessAlgorithm, dateTimeFormatter, bucketSize);
          LOG.info("Got {} buckets to process", bucketNameToBucketValueMS.size());

          // TODO: for datasources other than pinot, this will change
          // We can either implement a new algorithm for the external datasources
          // Or use query cache for the count * queries (cleaner method?)
          if (!bucketNameToBucketValueMS.isEmpty()) {
            // create current entries in database if not already present
            int numEntriesCreated = createEntriesInDatabaseIfNotPresent(dataset, bucketNameToBucketValueMS);
            LOG.info("Created {} new entries in database", numEntriesCreated);

            // coldstart: compute and store in db the counts for baseline, if not already present
            LOG.info("Checking for baseline counts in database, or fetching them if not present");
            dataCompletenessAlgorithm.computeBaselineCountsIfNotPresent(dataset, bucketNameToBucketValueMS,
                dateTimeFormatter, timeSpec, dateTimeZone);

            // get current counts for all current buckets to process
            Map<String, Long> bucketNameToCount =
                dataCompletenessAlgorithm.getCurrentCountsForBuckets(dataset, timeSpec, bucketNameToBucketValueMS);
            LOG.info("Bucket name to count {}", bucketNameToCount);

            // run completeness check for all buckets
            runCompletenessCheck(dataset, bucketNameToBucketValueMS, bucketNameToCount,
                dataCompletenessAlgorithm, expectedCompleteness);
          }

          // collect all older than expected delay, and still incomplete
          Period expectedDelayPeriod = datasetConfig.getExpectedDelay().toPeriod();
          long expectedDelayStart = new DateTime(adjustedEnd, dateTimeZone).minus(expectedDelayPeriod).getMillis();
          LOG.info("Expected delay for dataset {} is {}, checking from {} to {}",
              dataset, expectedDelayPeriod, adjustedStart, expectedDelayStart);
          if (adjustedStart < expectedDelayStart) {
            List<DataCompletenessConfigDTO> olderThanExpectedDelayAndNotComplete =
                DAO_REGISTRY.getDataCompletenessConfigDAO().findAllByDatasetAndInTimeRangeAndStatus(dataset, adjustedStart, expectedDelayStart, false);
            for (DataCompletenessConfigDTO entry : olderThanExpectedDelayAndNotComplete) {
              if (!entry.isDelayNotified()) {
                incompleteEntriesToNotify.put(dataset, entry);
                entry.setDelayNotified(true);
                DAO_REGISTRY.getDataCompletenessConfigDAO().update(entry);
              }
            }
          }
        } catch (Exception e) {
          LOG.error("Exception in data completeness checker task for dataset {}.. Continuing with remaining datasets", dataset, e);
        }
      }

      // notify
      LOG.info("Sending email notification for incomplete entries");
      if (!incompleteEntriesToNotify.isEmpty()) {
        EmailHelper.sendNotificationForDataIncomplete(incompleteEntriesToNotify, thirdeyeConfig);
      }

    } catch (Exception e) {
      LOG.error("Exception in data completeness checker task", e);
    }
  }


  /**
   * This task cleans up the database of data completeness config entries,
   * if they are older than constants CLEANUP_TIME_DURATION and CLEANUP_TIMEUNIT
   * @param dataCompletenessTaskInfo
   */
  private void executeCleanupTask(DataCompletenessTaskInfo dataCompletenessTaskInfo) {
    LOG.info("Execute data completeness cleanup {}", dataCompletenessTaskInfo);
    try {
      // find all entries older than 30 days, delete them
      long cleanupOlderThanDuration = TimeUnit.MILLISECONDS.convert(DataCompletenessConstants.CLEANUP_TIME_DURATION,
          DataCompletenessConstants.CLEANUP_TIMEUNIT);
      long cleanupOlderThanMillis = new DateTime().minus(cleanupOlderThanDuration).getMillis();
      List<DataCompletenessConfigDTO> findAllByTimeOlderThan =
          DAO_REGISTRY.getDataCompletenessConfigDAO().findAllByTimeOlderThan(cleanupOlderThanMillis);

      LOG.info("Deleting {} entries older than {} i.e. {}",
          findAllByTimeOlderThan.size(), cleanupOlderThanMillis, new DateTime(cleanupOlderThanMillis));
      List<Long> idsToDelete = new ArrayList<>();
      for (DataCompletenessConfigDTO config : findAllByTimeOlderThan) {
        idsToDelete.add(config.getId());
      }
      DAO_REGISTRY.getDataCompletenessConfigDAO().deleteByIds(idsToDelete);

      // find all entries older than LOOKBACK and still dataComplete=false, mark timedOut
      long timeOutOlderThanDuration = TimeUnit.MILLISECONDS.
          convert(DataCompletenessConstants.LOOKBACK_TIME_DURATION, DataCompletenessConstants.LOOKBACK_TIMEUNIT);
      long timeOutOlderThanMillis = new DateTime().minus(timeOutOlderThanDuration).getMillis();
      List<DataCompletenessConfigDTO> findAllByTimeOlderThanAndStatus =
          DAO_REGISTRY.getDataCompletenessConfigDAO().findAllByTimeOlderThanAndStatus(timeOutOlderThanMillis, false);

      LOG.info("Timing out {} entries older than {} i.e. {} and still not complete",
          findAllByTimeOlderThanAndStatus.size(), timeOutOlderThanMillis, new DateTime(timeOutOlderThanMillis));
      List<DataCompletenessConfigDTO> configToUpdate = new ArrayList<>();
      for (DataCompletenessConfigDTO config : findAllByTimeOlderThanAndStatus) {
        if (!config.isTimedOut()) {
          config.setTimedOut(true);
          configToUpdate.add(config);
        }
      }
      DAO_REGISTRY.getDataCompletenessConfigDAO().update(configToUpdate);
    } catch (Exception e) {
      LOG.error("Exception data completeness cleanup task", e);
    }
  }


  /**
   * Creates the current bucket entries in the table
   * @param dataset
   * @param bucketNameToBucketValueMS
   * @return
   */
  private int createEntriesInDatabaseIfNotPresent(String dataset, Map<String, Long> bucketNameToBucketValueMS) {
    int numEntriesCreated = 0;
    for (Entry<String, Long> entry : bucketNameToBucketValueMS.entrySet()) {
      String bucketName = entry.getKey();
      Long bucketValue = entry.getValue();
      DataCompletenessConfigDTO checkOrCreateConfig = DAO_REGISTRY.getDataCompletenessConfigDAO().findByDatasetAndDateSDF(dataset, bucketName);
      if (checkOrCreateConfig == null) {
        checkOrCreateConfig = new DataCompletenessConfigDTO();
        checkOrCreateConfig.setDataset(dataset);
        checkOrCreateConfig.setDateToCheckInSDF(bucketName);
        checkOrCreateConfig.setDateToCheckInMS(bucketValue);
        DAO_REGISTRY.getDataCompletenessConfigDAO().save(checkOrCreateConfig);
        numEntriesCreated++;
        // NOTE: Decided to not store timeValue in the DataCompletenessConfig, because one bucket can have multiple
        // timeValues (5 MINUTES bucketed into 30 MINUTES case)
      }
    }
    return numEntriesCreated;

  }

  /**
   * Gets all the buckets that need to be checked
   * @param dataset
   * @param adjustedStart
   * @param adjustedEnd
   * @param dataCompletenessAlgorithm
   * @param dateTimeFormatter
   * @param bucketSize
   * @return
   */
  private Map<String, Long> getBucketsToProcess(String dataset, long adjustedStart, long adjustedEnd,
      DataCompletenessAlgorithm dataCompletenessAlgorithm, DateTimeFormatter dateTimeFormatter, long bucketSize) {
    // find completed buckets from database in timerange, for dataset, and percentComplete >= 95%
    // We're using this call instead of checking for isDataComplete, because we want to check the entries
    // even after we marked it complete, in case the percentage changes
    // But instead of checking it for anything other than 100%, setting a limit called CONSIDER_COMPLETE_AFTER
    List<DataCompletenessConfigDTO> completeEntries =
        DAO_REGISTRY.getDataCompletenessConfigDAO().findAllByDatasetAndInTimeRangeAndPercentCompleteGT(
        dataset, adjustedStart, adjustedEnd, dataCompletenessAlgorithm.getConsiderCompleteAfter());
    List<String> completeBuckets = new ArrayList<>();
    for (DataCompletenessConfigDTO entry : completeEntries) {
      completeBuckets.add(entry.getDateToCheckInSDF());
    }
    LOG.info("Data complete buckets size:{} buckets:{}", completeBuckets.size(), completeBuckets);

    // get all buckets
    Map<String, Long> bucketNameToBucketValueMS = new HashMap<>(); // dateToCheckInSDF -> dateToCheckInMS
    while (adjustedStart < adjustedEnd) {
      String bucketName = dateTimeFormatter.print(adjustedStart);
      bucketNameToBucketValueMS.put(bucketName, adjustedStart);
      adjustedStart = adjustedStart + bucketSize;
    }
    LOG.info("All buckets size:{} buckets:{}", bucketNameToBucketValueMS.size(), bucketNameToBucketValueMS.keySet());

    // get buckets to process = all buckets - complete buckets
    bucketNameToBucketValueMS.keySet().removeAll(completeBuckets);
    LOG.info("Buckets to process = (All buckets - data complete buckets) size:{} buckets:{}",
        bucketNameToBucketValueMS.size(), bucketNameToBucketValueMS.keySet());

    return bucketNameToBucketValueMS;

  }

  /**
   * Checks completeness for each bucket of every dataset and updates the completeness percentage in the database
   * @param dataset
   * @param bucketNameToBucketValueMS
   * @param bucketNameToCount
   * @param dataCompletenessAlgorithm
   * @param expectedCompleteness
   */
  private void runCompletenessCheck(String dataset, Map<String, Long> bucketNameToBucketValueMS,
      Map<String, Long> bucketNameToCount, DataCompletenessAlgorithm dataCompletenessAlgorithm,
      Double expectedCompleteness) {
    Set<Entry<String, Long>> entries = bucketNameToBucketValueMS.entrySet();
    if (entries.size() > 0) {
      LOG.info("Checking {} completeness entries", entries.size());
    }
    for (Entry<String, Long> entry : entries) {
      String bucketName = entry.getKey();
      Long bucketValue = entry.getValue();
      Long currentCount = 0L;
      if(bucketNameToCount.containsKey(bucketName)) {
        currentCount = bucketNameToCount.get(bucketName);
      }
      LOG.info("Bucket name:{} Current count:{}", bucketName, currentCount);

      // get baseline counts for this bucket
      List<Long> baselineCounts = dataCompletenessAlgorithm.getBaselineCounts(dataset, bucketValue);
      LOG.info("Baseline counts:{}", baselineCounts);

      // call api with counts, algo, expectation
      double percentComplete = dataCompletenessAlgorithm.getPercentCompleteness(baselineCounts, currentCount);
      LOG.info("Percent complete:{}", percentComplete);

      // calculate if data is complete
      boolean dataComplete = dataCompletenessAlgorithm.isDataComplete(percentComplete, expectedCompleteness);
      LOG.info("IsDataComplete:{}", dataComplete);

      // update count, dataComplete, percentComplete, numAttempts in database
      DataCompletenessConfigDTO configToUpdate = DAO_REGISTRY.getDataCompletenessConfigDAO().findByDatasetAndDateSDF(dataset, bucketName);
      configToUpdate.setCountStar(currentCount);
      configToUpdate.setDataComplete(dataComplete);
      configToUpdate.setPercentComplete(Double.parseDouble(new DecimalFormat("##.##").format(percentComplete)));
      configToUpdate.setNumAttempts(configToUpdate.getNumAttempts() + 1);
      DAO_REGISTRY.getDataCompletenessConfigDAO().update(configToUpdate);
      LOG.info("Updated data completeness config id:{} with count *:{} dataComplete:{} percentComplete:{} "
          + "and numAttempts:{}", configToUpdate.getId(), configToUpdate.getCountStar(),
          configToUpdate.isDataComplete(), configToUpdate.getPercentComplete(), configToUpdate.getNumAttempts());
    }
  }



}
