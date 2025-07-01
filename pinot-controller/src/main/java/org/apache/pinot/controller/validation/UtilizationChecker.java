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
package org.apache.pinot.controller.validation;

import com.google.common.collect.BiMap;
import org.apache.pinot.controller.util.CompletionServiceHelper;


/**
 * Interface for all utilization checkers to be used to by ResourceUtilizationManager and ResourceUtilizationChecker
 */
public interface UtilizationChecker {
  /**
   * Get the name of the utilization checker
   */
  String getName();

  /**
   * Returns whether the resource's utilization is within limits
   * @param tableNameWithType table name with type
   * @param purpose purpose of this check
   * @return CheckResult, UNDETERMINED if result cannot be determined, PASS if within limits, FAIL if not within limits
   */
  CheckResult isResourceUtilizationWithinLimits(String tableNameWithType, CheckPurpose purpose);

  /**
   * Computes the resource's utilization
   * @param endpointsToInstances map of endpoints to instances
   * @param completionServiceHelper the completion service helper
   */
  void computeResourceUtilization(BiMap<String, String> endpointsToInstances,
      CompletionServiceHelper completionServiceHelper);

  /**
   * Passed to 'isResourceUtilizationWithinLimits' so that each 'UtilizationChecker' can decide if any special handling
   * is required depending on the origin of the check
   */
  enum CheckPurpose {
    // REALTIME_INGESTION if the check is performed from the realtime ingestion code path to pause ingestion
    // TASK_GENERATION if the check is performed from the task generation framework to pause creation of new tasks
    REALTIME_INGESTION, TASK_GENERATION
  }

  enum CheckResult {
    // PASS if the resource's utilization is within limits
    // FAIL if the resource's utilization is not within limits
    // UNDETERMINED if the result cannot be determined due to not having sufficient information
    PASS, FAIL, UNDETERMINED
  }
}
