/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.controller.ControllerJobType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerZkHelixUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerZkHelixUtils.class);

  private ControllerZkHelixUtils() {
    // Utility class
  }

  /**
   * Adds a new job metadata entry for a controller job like table rebalance or segment reload into ZK
   *
   * @param propertyStore the ZK property store to write to
   * @param jobId job's UUID
   * @param jobMetadata the job metadata
   * @param jobType the controller job type
   * @param prevJobMetadataChecker to check the previous job metadata before adding new one
   * @return boolean representing success / failure of the ZK write step
   */
  public static boolean addControllerJobToZK(ZkHelixPropertyStore<ZNRecord> propertyStore, String jobId,
      Map<String, String> jobMetadata, ControllerJobType jobType,
      Predicate<Map<String, String>> prevJobMetadataChecker) {
    Preconditions.checkState(jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS) != null,
        CommonConstants.ControllerJob.SUBMISSION_TIME_MS
            + " in JobMetadata record not set. Cannot expire these records");
    String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType.name());
    Stat stat = new Stat();
    ZNRecord jobsZnRecord = propertyStore.get(jobResourcePath, stat, AccessOption.PERSISTENT);
    if (jobsZnRecord != null) {
      Map<String, Map<String, String>> jobMetadataMap = jobsZnRecord.getMapFields();
      Map<String, String> prevJobMetadata = jobMetadataMap.get(jobId);
      if (!prevJobMetadataChecker.test(prevJobMetadata)) {
        return false;
      }
      jobMetadataMap.put(jobId, jobMetadata);
      jobMetadataMap = expireControllerJobsInZk(jobMetadataMap, jobType);
      jobsZnRecord.setMapFields(jobMetadataMap);
      return propertyStore.set(jobResourcePath, jobsZnRecord, stat.getVersion(), AccessOption.PERSISTENT);
    } else {
      jobsZnRecord = new ZNRecord(jobResourcePath);
      jobsZnRecord.setMapField(jobId, jobMetadata);
      return propertyStore.set(jobResourcePath, jobsZnRecord, AccessOption.PERSISTENT);
    }
  }

  /**
   * Get all controller jobs from ZK for a given set of job types.
   * @param jobTypes the set of job types to filter
   * @param jobMetadataChecker a predicate to filter the job metadata
   * @param propertyStore the ZK property store to read from
   * @return a map of jobId to job metadata for all the jobs that match the given job types and metadata checker
   */
  public static Map<String, Map<String, String>> getAllControllerJobs(Set<ControllerJobType> jobTypes,
      Predicate<Map<String, String>> jobMetadataChecker, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    Map<String, Map<String, String>> controllerJobs = new HashMap<>();
    for (ControllerJobType jobType : jobTypes) {
      String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType.name());
      ZNRecord jobsZnRecord = propertyStore.get(jobResourcePath, null, AccessOption.PERSISTENT);
      if (jobsZnRecord == null) {
        continue;
      }
      Map<String, Map<String, String>> jobMetadataMap = jobsZnRecord.getMapFields();
      for (Map.Entry<String, Map<String, String>> jobMetadataEntry : jobMetadataMap.entrySet()) {
        String jobId = jobMetadataEntry.getKey();
        Map<String, String> jobMetadata = jobMetadataEntry.getValue();
        Preconditions.checkState(jobMetadata.get(CommonConstants.ControllerJob.JOB_TYPE).equals(jobType.name()),
            "Got unexpected jobType: %s at jobResourcePath: %s with jobId: %s", jobType, jobResourcePath, jobId);
        if (jobMetadataChecker.test(jobMetadata)) {
          controllerJobs.put(jobId, jobMetadata);
        }
      }
    }
    return controllerJobs;
  }

  /**
   * Expires controller jobs in ZK if the number of jobs exceeds the configured limit for the job type.
   * The jobs are sorted by submission time, and the oldest inactive jobs are removed first.
   *
   * @param jobMetadataMap the map of job metadata entries
   * @param jobType the controller job type
   * @return the updated map of job metadata entries after expiring old inactive jobs
   */
  @VisibleForTesting
  static Map<String, Map<String, String>> expireControllerJobsInZk(Map<String, Map<String, String>> jobMetadataMap,
      ControllerJobType jobType) {
    if (jobMetadataMap.size() <= jobType.getZkNumJobsLimit()) {
      // No need to expire any jobs
      return jobMetadataMap;
    }

    Map<String, Map<String, String>> sortedJobMetadataMap = jobMetadataMap.entrySet().stream()
        .sorted(Comparator.comparingLong(
            v -> Long.parseLong(v.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS))))
        .collect(
            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldVal, newVal) -> oldVal, LinkedHashMap::new));

    int numToDelete = sortedJobMetadataMap.size() - jobType.getZkNumJobsLimit();

    Iterator<Map.Entry<String, Map<String, String>>> iterator = sortedJobMetadataMap.entrySet().iterator();
    while (iterator.hasNext() && numToDelete > 0) {
      Map.Entry<String, Map<String, String>> jobMetadataEntry = iterator.next();
      if (jobType.canDelete(Pair.of(jobMetadataEntry.getKey(), jobMetadataEntry.getValue()))) {
        iterator.remove();
        numToDelete--;
      }
    }

    if (numToDelete > 0) {
      LOGGER.warn(
          "The number of controller jobs in ZK for job type {} is {}, which exceeds the limit of {}. Some jobs could "
              + "not be expired because they are still in progress.",
          jobType, sortedJobMetadataMap.size(), jobType.getZkNumJobsLimit());
    }

    return sortedJobMetadataMap;
  }
}
