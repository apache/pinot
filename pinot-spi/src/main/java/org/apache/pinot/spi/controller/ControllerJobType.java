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
package org.apache.pinot.spi.controller;

import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Interface for controller job types that store metadata in the ZK property store.
 */
public interface ControllerJobType {

  /**
   * Name of the controller job type, which is used in the ZK property store path for storing job metadata for jobs
   * of this type.
   */
  String name();

  /**
   * Gets the maximum number of job metadata entries that can be stored in ZK for this job type.
   */
  default Integer getZkNumJobsLimit() {
    return CommonConstants.ControllerJob.DEFAULT_MAXIMUM_CONTROLLER_JOBS_IN_ZK;
  }

  /**
   * Checks if the job metadata entry can be safely deleted. Note that the job metadata entry will only be attempted
   * to be deleted when the number of entries in the job metadata map exceeds the configured limit for the job type.
   *
   * @param jobMetadataEntry The job metadata entry to check - a pair of job ID and job metadata map
   * @return true if the job metadata entry can be safely deleted, false otherwise
   */
  default boolean canDelete(Pair<String, Map<String, String>> jobMetadataEntry) {
    return true;
  }
}
