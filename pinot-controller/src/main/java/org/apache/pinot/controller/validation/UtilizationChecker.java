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
   * Returns true if the resource's utilization is within limits
   * @param tableNameWithType table name with type
   * @param isForMinion should be true if called from the minion task generation framework
   */
  boolean isResourceUtilizationWithinLimits(String tableNameWithType, boolean isForMinion);

  /**
   * Computes the resource's utilization
   */
  void computeResourceUtilization(BiMap<String, String> endpointsToInstances,
      CompletionServiceHelper completionServiceHelper);
}
