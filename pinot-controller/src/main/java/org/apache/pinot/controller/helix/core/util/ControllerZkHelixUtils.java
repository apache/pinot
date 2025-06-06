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

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.zookeeper.data.Stat;


public class ControllerZkHelixUtils {

  private ControllerZkHelixUtils() {
    // Utility class
  }

  /**
   * Adds a new job metadata entry for a controller job like table rebalance or segment reload into ZK
   *
   * @param propertyStore the ZK property store to write to
   * @param jobId job's UUID
   * @param jobMetadata the job metadata
   * @param jobType the type of the job to figure out where the job metadata is kept in ZK
   * @param prevJobMetadataChecker to check the previous job metadata before adding new one
   * @return boolean representing success / failure of the ZK write step
   */
  public static boolean addControllerJobToZK(ZkHelixPropertyStore<ZNRecord> propertyStore, String jobId,
      Map<String, String> jobMetadata, String jobType, Predicate<Map<String, String>> prevJobMetadataChecker) {
    Preconditions.checkState(jobMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS) != null,
        CommonConstants.ControllerJob.SUBMISSION_TIME_MS
            + " in JobMetadata record not set. Cannot expire these records");
    String jobResourcePath = ZKMetadataProvider.constructPropertyStorePathForControllerJob(jobType);
    Stat stat = new Stat();
    ZNRecord jobsZnRecord = propertyStore.get(jobResourcePath, stat, AccessOption.PERSISTENT);
    if (jobsZnRecord != null) {
      Map<String, Map<String, String>> jobMetadataMap = jobsZnRecord.getMapFields();
      Map<String, String> prevJobMetadata = jobMetadataMap.get(jobId);
      if (!prevJobMetadataChecker.test(prevJobMetadata)) {
        return false;
      }
      jobMetadataMap.put(jobId, jobMetadata);
      if (jobMetadataMap.size() > CommonConstants.ControllerJob.MAXIMUM_CONTROLLER_JOBS_IN_ZK) {
        jobMetadataMap = jobMetadataMap.entrySet().stream().sorted((v1, v2) -> Long.compare(
                Long.parseLong(v2.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS)),
                Long.parseLong(v1.getValue().get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS))))
            .collect(Collectors.toList()).subList(0, CommonConstants.ControllerJob.MAXIMUM_CONTROLLER_JOBS_IN_ZK)
            .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
      jobsZnRecord.setMapFields(jobMetadataMap);
      return propertyStore.set(jobResourcePath, jobsZnRecord, stat.getVersion(), AccessOption.PERSISTENT);
    } else {
      jobsZnRecord = new ZNRecord(jobResourcePath);
      jobsZnRecord.setMapField(jobId, jobMetadata);
      return propertyStore.set(jobResourcePath, jobsZnRecord, AccessOption.PERSISTENT);
    }
  }
}
